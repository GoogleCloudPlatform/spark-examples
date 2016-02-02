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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LeaderBoard extends HourlyTeamScore {

  static final Duration FIVE_MINUTES = Durations.minutes(5);
  static final Duration TEN_MINUTES = Durations.minutes(10);

  public static class LeaderBoardOptions extends HourlyTeamScoreOptions {
    public LeaderBoardOptions() {
      options.addOption("topic", true, "Pub/Sub topic to read from");
      options.addOption(
          "teamWindowDuration",
          true,
          "Numeric value of fixed window duration for team analysis, in minutes");
      options.addOption(
          "allowedLateness", true, "Numeric value of allowed data lateness, in minutes");
      options.addOption(
          "numReceivers",
          true,
          "Number of spark streaming Receivers to pull inputs from Pub/Sub concurrently");
    }

    public String getTopic() {
      return getOptionWithDefaultValue("topic", "gaming");
    }

    public Integer getTeamWindowDuration() {
      return Integer.valueOf(getOptionWithDefaultValue("teamWindowDuration", "60"));
    }

    public Integer getAllowedLateness() {
      return Integer.valueOf(getOptionWithDefaultValue("allowedLateness", "120"));
    }

    public String getTableName() {
      return getOptionWithDefaultValue("tableName", "leaderboard");
    }

    public Integer getNumReceivers() {
      return Integer.valueOf(getOptionWithDefaultValue("numReceivers", "1"));
    }
  }

  public static class RDDTableWriter extends BigqueryWriter {
    public <K, V> Void write(
        JavaPairRDD<K, V> rdd, Function<Tuple2<K, V>, TableRow> convertToTableRow) {
      rdd.foreach(row -> this.writeRow(convertToTableRow.call(row)));
      return null;
    }
  }

  // Helper to convert (team, score) tuples to JSON objects, which is what we
  // need to output them to Bigquery.
  private static class ConvertToTeamTableRow
      implements Function<Tuple2<WithTimestamp<String>, WithTimestamp<Integer>>, TableRow> {
    private Long windowDurationMs;

    public ConvertToTeamTableRow(Long windowDurationMs) {
      this.windowDurationMs = windowDurationMs;
    }

    public TableRow call(Tuple2<WithTimestamp<String>, WithTimestamp<Integer>> pairs) {
      Long processingTime = System.currentTimeMillis();
      TableRow tableRow =
          new TableRow()
              .set("team", pairs._1().val())
              .set("window_start", timestampFormatter.print(pairs._1().timestamp()))
              .set("total_score", pairs._2().val())
              .set("processing_time", timestampFormatter.print(processingTime));
      if (processingTime < pairs._1().timestamp() + windowDurationMs) {
        tableRow.set("timing", "ON_TIME");
      } else {
        tableRow.set("timing", "LATE");
      }
      return tableRow;
    }
  }

  // Helper to convert (user, score) tuples to JSON objects, which is what we
  // need to output them to Bigquery.
  private static Function<Tuple2<String, WithTimestamp<Integer>>, TableRow> convertToUserTableRow =
      (pairs)
          -> new TableRow()
              .set("user", pairs._1())
              .set("total_score", pairs._2().val())
              .set("processing_time", timestampFormatter.print(System.currentTimeMillis()));

  private static TableSchema getTeamTableSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("team").setType("STRING"));
    fields.add(new TableFieldSchema().setName("window_start").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("STRING"));
    fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
    return new TableSchema().setFields(fields);
  }

  private static TableSchema getUserTableSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("user").setType("STRING"));
    fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("processing_time").setType("STRING"));
    return new TableSchema().setFields(fields);
  }

  private static class SumAggregator
      implements Function2<
          List<Integer>, Optional<WithTimestamp<Integer>>, Optional<WithTimestamp<Integer>>> {
    final private static WithTimestamp<Integer> INITIAL_STATE = WithTimestamp.create(0, 0L);
    private Long TTL = Long.MAX_VALUE; // All the State with window_start < TTL will be purged.

    SumAggregator setTTL(Long TTL) {
      this.TTL = TTL;
      return this;
    }

    public Optional<WithTimestamp<Integer>> call(
        List<Integer> scores, Optional<WithTimestamp<Integer>> state) {

      // Purge out of date entries
      final Long cutoffTime = System.currentTimeMillis() - TTL;
      if (state.isPresent() && state.get().timestamp() < cutoffTime) {
        return Optional.absent();
      }

      if (scores.size() == 0) return state;

      WithTimestamp<Integer> sumWithTimestamp = state.or(INITIAL_STATE);
      Integer sum = sumWithTimestamp.val() + scores.stream().mapToInt(Integer::intValue).sum();
      return Optional.of(WithTimestamp.create(sum, System.currentTimeMillis()));
    };
  }

  // to track the last timestamp of the current spark microbatch
  private static final AtomicLong teamWindowTimestamp = new AtomicLong(0); // For team stats
  private static final AtomicLong userWindowTimestamp = new AtomicLong(0); // For user stats

  public static void main(String[] args) throws Exception {
    LeaderBoardOptions options = new LeaderBoardOptions();
    options.parse(args);

    final String project = options.getProject();
    final String dataset = options.getDataset();
    final String userTableName = options.getTableName() + "_user_score";
    final String teamTableName = options.getTableName() + "_team_score";

    // Create Bigquery Tables if they do not exist
    RDDTableWriter teamTableWriter = new RDDTableWriter();
    teamTableWriter
        .asProject(project)
        .toDataset(dataset)
        .toTable(teamTableName)
        .withSchema(getTeamTableSchema())
        .createTable();

    RDDTableWriter userTableWriter = new RDDTableWriter();
    userTableWriter
        .asProject(project)
        .toDataset(dataset)
        .toTable(userTableName)
        .withSchema(getUserTableSchema())
        .createTable();

    SparkConf sparkConf = new SparkConf().setAppName("LeaderBoard");
    // Update with refresh interval of 5 minutes
    StreamingContext ssc = new StreamingContext(sparkConf, FIVE_MINUTES);
    JavaStreamingContext jsc = new JavaStreamingContext(ssc);
    // Checkpointing must be enabled to use the updateStateByKey function.
    jsc.checkpoint("/tmp/spark-leaderboard/" + ssc.sparkContext().applicationId());

    // Create a unified DStream with specificed number of Receivers.
    final int numReceivers = options.getNumReceivers();
    List<JavaDStream<String>> pubsubStreams = new ArrayList<>(numReceivers);
    for (int i = 0; i < numReceivers; i++) {
      pubsubStreams.add(
          jsc.receiverStream(
              new CloudPubsubReceiver(
                  options.getProject(),
                  options.getTopic(),
                  "spark-leaderboard-" + ssc.sparkContext().applicationId())));
    }
    JavaDStream<String> lines =
        jsc.union(pubsubStreams.get(0), pubsubStreams.subList(1, pubsubStreams.size()));

    // Parse the incoming data
    JavaDStream<GameActionInfo> gameEvents = lines.flatMap(new ParseEventFn());
    gameEvents.persist(StorageLevel.MEMORY_AND_DISK_SER());

    final Long allowedLatenessMs = Durations.minutes(options.getAllowedLateness()).milliseconds();
    final Long teamWindowDurationMs =
        Durations.minutes(options.getTeamWindowDuration()).milliseconds();

    JavaPairDStream<WithTimestamp<String>, Integer> teamScoreBatch =
        gameEvents
            // Discard data arrived after the allowedLateness timeline
            .filter(
                gInfo
                    -> gInfo.getTimestamp()
                        > System.currentTimeMillis() - allowedLatenessMs - teamWindowDurationMs)
            .mapToPair(
                gInfo
                    -> new Tuple2<>(
                        // Extract the composite key (teamname, window_start)
                        WithTimestamp.create(
                            gInfo.getTeam(),
                            // Apply Fixed Window by rounding the timestamp down to the nearest
                            // multiple of the window size
                            (gInfo.getTimestamp() / teamWindowDurationMs) * teamWindowDurationMs),
                        // Extract the team scores as the Values
                        gInfo.getScore()))
            // Compute the sum of the scores per team per window
            .reduceByKey(new SumScore());

    // Key = (team, window_start), Value = (total_score, processing_time)
    JavaPairDStream<WithTimestamp<String>, WithTimestamp<Integer>> hourlyTeamScore =
        teamScoreBatch
            // Extract the maximum of the RDD window end times as the start time for processing.
            // Unfortunately, Spark Java does not provide direct access but through transform().
            .transformToPair(
                (rdd, timestamp) -> {
                  teamWindowTimestamp.set(
                      Math.max(teamWindowTimestamp.get(), timestamp.milliseconds()));
                  return rdd;
                })
            // Merge the result from this batch into the toal_score in the stateful RDD
            .updateStateByKey(
                new SumAggregator()
                    // And remove those windows we do not need to track.
                    .setTTL(allowedLatenessMs + teamWindowDurationMs));

    hourlyTeamScore
        // Only output those scores updated in this batch, i.e. processingTime >= end time of
        // the current window
        .filter(x -> x._2().timestamp() >= teamWindowTimestamp.get())
        .foreachRDD(
            rdd -> teamTableWriter.write(rdd, new ConvertToTeamTableRow(teamWindowDurationMs)));

    // Key: username Value: total_score, processing_time when the score is updated.
    JavaPairDStream<String, WithTimestamp<Integer>> userScores =
        gameEvents
            // Update every 10 minutes
            .window(TEN_MINUTES, TEN_MINUTES)
            // Exactly the same operations as in UserScore.java
            .mapToPair(new ExtractUserScore())
            .reduceByKey(new SumScore())
            // Extract the maximum of the RDD window end times as the start time for processing.
            // Unfortunately, Spark Java (as version 1.5.2) does not provide direct access but only
            // through transform().
            .transformToPair(
                (rdd, timestamp) -> {
                  userWindowTimestamp.set(
                      Math.max(userWindowTimestamp.get(), timestamp.milliseconds()));
                  return rdd;
                })
            // Additionally, aggregate the result with the prior sum from earlier input batches.
            .updateStateByKey(new SumAggregator());

    userScores
        // Only output those scores updated in this batch
        .filter(x -> x._2().timestamp() >= userWindowTimestamp.get())
        .foreachRDD(rdd -> userTableWriter.write(rdd, convertToUserTableRow));

    jsc.start();
    jsc.awaitTermination();
  }
}
