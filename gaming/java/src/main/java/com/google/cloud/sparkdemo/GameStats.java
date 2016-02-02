/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * This is not an official Google product.
 */

package com.google.cloud.sparkdemo;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.io.Serializable;
import java.lang.Iterable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GameStats extends LeaderBoard {

  static final Duration ONE_MINUTE = Durations.minutes(1);

  public static class GameStatsOptions extends LeaderBoardOptions {
    public GameStatsOptions() {
      options.addOption("topic", true, "Pub/Sub topic to read from");
      options.addOption(
          "fixedWindowDuration",
          true,
          "Numeric value of fixed window duration for user analysis, in minutes");
      options.addOption(
          "userActivityWindowDuration",
          true,
          "Numeric value of fixed window for finding mean of user session duration, in minutes");
      options.addOption(
          "sessionGap", true, "Numeric value of gap between user sessions, in minutes");
    }

    public String getTopic() {
      return getOptionWithDefaultValue("topic", "gaming");
    }

    public Integer getFixedWindowDuration() {
      return Integer.valueOf(getOptionWithDefaultValue("fixedWindowDuration", "60"));
    }

    public Integer getUserActivityWindowDuration() {
      return Integer.valueOf(getOptionWithDefaultValue("userActivityWindowDuration", "30"));
    }

    public Integer getSessionGap() {
      return Integer.valueOf(getOptionWithDefaultValue("sessionGap", "5"));
    }

    public String getTableName() {
      return getOptionWithDefaultValue("tableName", "game_stats");
    }
  }

  // Helper to convert (team, score) tuples to JSON objects, which is what we
  // need to output them to Bigquery.
  private static Function<Tuple2<String, Integer>, TableRow> convertToTeamScoreRow =
      (pairs)
          -> new TableRow()
              .set("team", pairs._1())
              .set("total_score", pairs._2())
              .set("processing_time", timestampFormatter.print(System.currentTimeMillis()));

  // Helper to convert (window_start, (session_duration_sum, session_count)) tuples to JSON
  // objects, which is what we need to output them to Bigquery.
  private static Function<Tuple2<Long, Double>, TableRow> convertToSessionTableRow =
      (pairs)
          -> new TableRow()
              .set("window_start", timestampFormatter.print(pairs._1()))
              .set("mean_duration", pairs._2())
              .set("processing_time", timestampFormatter.print(System.currentTimeMillis()));

  private static TableSchema getTeamTableSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("team").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("STRING"));
    return new TableSchema().setFields(fields);
  }

  /** Build the output table schema. */
  private static TableSchema getSessionTableSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("window_start").setType("STRING"));
    fields.add(new TableFieldSchema().setName("mean_duration").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("STRING"));
    return new TableSchema().setFields(fields);
  }

  // A Session is represented by the timestamps of the start event and the end event.
  public static class Session implements Serializable {
    public Long start;
    public Long end;
    public boolean closed = false;

    Session(Long start, Long end) {
      this.start = start;
      this.end = end;
    }

    // Merge the other session in this one.
    public void merge(Session other) {
      this.start = Math.min(this.start, other.start);
      this.end = Math.max(this.end, other.end);
    }

    public void close() {
      this.closed = true;
    }

    public String toString() {
      return "(" + start + ", " + end + ")";
    }
  }

  private static class SessionizeUserActivities
      implements Function2<List<Long>, Optional<List<Session>>, Optional<List<Session>>> {
    private Long sessionGap; // Minimum gap between sessions.
    private Long TTL; // All the Sessions with ending time < TTL will be purged.

    SessionizeUserActivities setGap(Long sessionGap) {
      this.sessionGap = sessionGap;
      return this;
    }

    SessionizeUserActivities setTTL(Long TTL) {
      this.TTL = TTL;
      return this;
    }

    private void closeIfExpired(Session session, long cutoffTime) {
      if (session.end <= cutoffTime)
        session.close();
    }

    // Sort all the sessions by the session end time in ascending order
    // if s_[i+1].start < s_i.end + gap
    //    merge two sessions into (min(s_[i+1].start, s_[i].start), s_[i+1].end))
    // else
    //    emit s[i]
    public Optional<List<Session>> call(List<Long> timestamps, Optional<List<Session>> state) {
      // Filter out closed sessions from the previous batch.
      List<Session> sessions = state.or(new ArrayList<Session>())
          .stream()
          .filter(session -> !session.closed)
          .collect(Collectors.toList());
      final Long cutoffTime = System.currentTimeMillis() - TTL;

      // Add all new event timestamps as individual sessions
      for (Long ts : timestamps) {
        sessions.add(new Session(ts, ts));
      }

      if (sessions.size() == 0) return Optional.absent();

      List<Session> mergedSessions = new ArrayList<>();
      // Sort all sessions by the ascending order of session end times.
      sessions.sort((a, b) -> (a.end.compareTo(b.end)));
      Session current = sessions.get(0);
      for (Session next : sessions.subList(1, sessions.size())) {
        if (next.start < current.end + sessionGap) {
          // Sessions are Overlapped. Merge them accordingly
          current.merge(next);
        } else {
          closeIfExpired(current, cutoffTime);
          mergedSessions.add(current);
          current = next;
        }
      }
      closeIfExpired(current, cutoffTime);
      mergedSessions.add(current);

      if (mergedSessions.size() == 0) return Optional.absent();

      return Optional.of(mergedSessions);
    };
  }

  // Calculate the session length from the session states for each user.
  private static class SessionDurationByWindow
      implements PairFlatMapFunction<Tuple2<String, List<Session>>, Long, Tuple2<Long, Integer>> {
    private Long windowDuration;

    SessionDurationByWindow(Long windowDuration) {
      this.windowDuration = windowDuration;
    }

    public Iterable<Tuple2<Long, Tuple2<Long, Integer>>> call(
        Tuple2<String, List<Session>> userSessions) {
      return userSessions
          ._2()
          .stream()
          .map(
              session
                  -> new Tuple2<Long, Tuple2<Long, Integer>>(
                      (session.end / windowDuration) * windowDuration,
                      new Tuple2<Long, Integer>(session.end - session.start, 1)))
          .collect(Collectors.toList());
    }
  }

  /**
   * Filter out all but those users with a high clickrate, which we will consider as 'spammy' uesrs.
   * We do this by finding the mean total score per user, then using that information as a side
   * input to filter out all but those user scores that are > (mean * SCORE_WEIGHT)
   */
  public static class CalculateSpammyUsers
      implements Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>> {
    private static final double SCORE_WEIGHT = 2.5;

    @Override
    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> userScores) {

      // Get the sum of scores for each user.
      JavaPairRDD<String, Integer> sumScores = userScores.reduceByKey((a, b) -> (a + b));

      // Extract the score from each element, and use it to find the global mean.
      Double globalMeanScore = sumScores.mapToDouble(x -> x._2()).mean();

      // Filter the user sums using the global mean.
      JavaPairRDD<String, Integer> filtered =
          sumScores.filter(
              sumByUser -> {
                if (sumByUser._2() > globalMeanScore * SCORE_WEIGHT) {
                  return true;
                } else return false;
              });

      return filtered;
    }
  }

  public static void main(String[] args) throws Exception {
    GameStatsOptions options = new GameStatsOptions();
    options.parse(args);

    final String project = options.getProject();
    final String dataset = options.getDataset();
    final String sessionTableName = options.getTableName() + "_sessions";
    final String teamTableName = options.getTableName() + "_team_score";

    // Create Bigquery Tables if they do not exist
    RDDTableWriter teamTableWriter = new RDDTableWriter();
    teamTableWriter
        .asProject(project)
        .toDataset(dataset)
        .toTable(teamTableName)
        .withSchema(getTeamTableSchema())
        .createTable();

    RDDTableWriter sessionTableWriter = new RDDTableWriter();
    sessionTableWriter
        .asProject(project)
        .toDataset(dataset)
        .toTable(sessionTableName)
        .withSchema(getSessionTableSchema())
        .createTable();

    SparkConf sparkConf = new SparkConf().setAppName("GameStats");

    //Update with refresh interval of 5 minutes
    StreamingContext ssc = new StreamingContext(sparkConf, ONE_MINUTE);
    JavaStreamingContext jsc = new JavaStreamingContext(ssc);
    // Checkpointing must be enabled to use the updateStateByKey function.
    jsc.checkpoint("/tmp/spark-game-stats/" + ssc.sparkContext().applicationId());

    // Create a unified DStream with specificed number of Receivers.
    final int numReceivers = options.getNumReceivers();
    List<JavaDStream<String>> pubsubStreams = new ArrayList<>(numReceivers);
    for (int i = 0; i < numReceivers; i++) {
      pubsubStreams.add(
          jsc.receiverStream(
              new CloudPubsubReceiver(
                  options.getProject(),
                  options.getTopic(),
                  "spark-gamestats-" + ssc.sparkContext().applicationId())));
    }
    JavaDStream<String> lines =
        jsc.union(pubsubStreams.get(0), pubsubStreams.subList(1, pubsubStreams.size()));

    // Parse the incoming data
    JavaDStream<GameActionInfo> rawEvents = lines.flatMap(new ParseEventFn());
    rawEvents.persist(StorageLevel.MEMORY_AND_DISK_SER());

    // Extract username/score pairs from the event stream
    // over the fixed windows
    JavaDStream<GameActionInfo> userEvents =
        rawEvents.window(
            Durations.minutes(options.getFixedWindowDuration()),
            Durations.minutes(options.getFixedWindowDuration()));

    // Calculate the total score per user over fixed windows, and identify those
    // with (SCORE_WEIGHT * avg) clickrate. These might be robots/spammers.
    JavaPairDStream<String, Integer> spammersView =
        userEvents
            .mapToPair((GameActionInfo gInfo) -> new Tuple2<>(gInfo.getUser(), gInfo.getScore()))
            .transformToPair(new CalculateSpammyUsers());

    // Calculate the total score per team over the same fixed windows,
    // Uses the spammer list derived above-- the set of suspected robots-- to filter out
    // scores from those users from the sum.
    userEvents
        // With "cogroup" op, construct KVs as user -> <Iterable<GameActionInfo>, Iterable<Integer>>
        .mapToPair((GameActionInfo gInfo) -> new Tuple2<>(gInfo.getUser(), gInfo))
        .cogroup(spammersView)
        // Filter out those entries with username in the spammer list.
        .filter(x -> !x._2()._2().iterator().hasNext())
        // Flatten to a RDD of GameActionInfos
        .flatMap(x -> x._2()._1())
        // Re-map to have "team" as the key and calculate the sum
        .mapToPair((GameActionInfo gInfo) -> new Tuple2<>(gInfo.getTeam(), gInfo.getScore()))
        .reduceByKey(new SumScore())
        // Write the result to BigQuery
        .foreachRDD(rdd -> teamTableWriter.write(rdd, convertToTeamScoreRow));

    final Long userActivityWindowDurationMs =
        Durations.minutes(options.getUserActivityWindowDuration()).milliseconds();
    final Long allowedLatenessMs = Durations.minutes(options.getAllowedLateness()).milliseconds();
    final Long sessionGapMs = Durations.minutes(options.getSessionGap()).milliseconds();

    // Calculate the total score for the users per session-- that is, a burst of activity
    // separated by a gap from further activity. Find and record the mean session lengths.
    // This information could help the game designers track the changing user engagement
    // as their set of games changes.
    // Key: username  Val: List of sessions, including session start and end times.
    JavaPairDStream<String, List<Session>> userSessions =
        rawEvents
            .window(
                Durations.minutes(options.getUserActivityWindowDuration()),
                Durations.minutes(options.getUserActivityWindowDuration()))
            // Extract the user event timestamps.
            .mapToPair(
                (GameActionInfo gInfo) -> new Tuple2<>(gInfo.getUser(), gInfo.getTimestamp()))
            // Update the session states
            .updateStateByKey(
                new SessionizeUserActivities()
                    .setGap(sessionGapMs)
                    .setTTL(allowedLatenessMs + userActivityWindowDurationMs))
            // Filter out open sessions so we only emit closed sessions.
            .mapValues(sessions -> sessions.stream()
                .filter(s -> s.closed)
                .collect(Collectors.toList()));

    // Key: window_start Val: sum of session durations, and # of sessions.
    JavaPairDStream<Long, Double> meanSessionDurations =
        userSessions
            // Extract the session durations from the list for each user,
            // for each fixed window.
            .flatMapToPair(new SessionDurationByWindow(userActivityWindowDurationMs))
            // Compute the overall sum and count so we can compute the mean session duration
            .reduceByKey((a, b) -> new Tuple2<Long, Integer>(a._1() + b._1(), a._2() + b._2()))
            // Compute mean duration as Double value
            .mapValues(v -> v._1().doubleValue() / v._2() / 60.0 / 1000.0);

    // Write to bigquery
    meanSessionDurations.foreachRDD(rdd -> sessionTableWriter.write(rdd, convertToSessionTableRow));

    jsc.start();
    jsc.awaitTermination();
  }
}
