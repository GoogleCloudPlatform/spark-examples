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

import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import scala.Tuple2;

import java.util.TimeZone;

public class HourlyTeamScore extends UserScore {
  public final static DateTimeFormatter timestampFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  private final static DateTimeFormatter timestampParser =
      DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

  public static class HourlyTeamScoreOptions extends UserScoreOptions {
    public HourlyTeamScoreOptions() {
      options.addOption(
          "windowDuration", true, "Numeric value of fixed window duration, in minutes");
      options.addOption(
          "startMin",
          true,
          "String representation of the first minute for which to generate results,"
              + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
              + "Any input data timestamped prior to that minute won't be included in the sums.");
      options.addOption(
          "stopMin",
          true,
          "String representation of the first minute for which to not generate results,"
              + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
              + "Any input data timestamped after that minute won't be included in the sums.");
    }

    public String getStartMin() {
      return getOptionWithDefaultValue("startMin", "1970-01-01-00-00");
    }

    public String getStopMin() {
      return getOptionWithDefaultValue("stopMin", "2100-01-01-00-00");
    }

    public Long getWindowDuration() {
      return Long.valueOf(getOptionWithDefaultValue("windowDuration", "60"));
    }

    public String getTableName() {
      return getOptionWithDefaultValue("tableName", "Hourly");
    }

    public String getTableSchema() {
      return "["
          + "{'name': 'team','type': 'STRING'},"
          + "{'name': 'total_score','type': 'INTEGER'},"
          + "{'name': 'window_start','type': 'STRING'}"
          + "]";
    }
  }

  // A helper generic for attaching timestamp to a given value
  public static class WithTimestamp<T> extends Tuple2<T, Long> {
    WithTimestamp(T val, Long timestamp) {
      super(val, timestamp);
    }

    T val() {
      return _1();
    }

    Long timestamp() {
      return _2();
    }

    public static <T> WithTimestamp<T> create(T val, Long timestamp) {
      return new WithTimestamp<T>(val, timestamp);
    }
  }

  // Helper to convert <<team, window_start>, score> tuples to JSON objects, which is what we
  // need to output them to BigQuery.
  private static PairFunction<Tuple2<WithTimestamp<String>, Integer>, String, JsonObject>
      convertToJson =
          (pairs) -> {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("team", pairs._1().val());
            jsonObject.addProperty("total_score", pairs._2());
            jsonObject.addProperty(
                "window_start", timestampFormatter.print(pairs._1().timestamp()));
            return new Tuple2<String, JsonObject>(null, jsonObject);
          };

  /**
   * Run a batch pipeline.
   **/
  public static void main(String[] args) throws Exception {
    HourlyTeamScoreOptions options = new HourlyTeamScoreOptions();
    options.parse(args);

    SparkConf sc = new SparkConf().setAppName("HourlyTeamScore");
    JavaSparkContext jsc = new JavaSparkContext(sc);

    Configuration hadoopConf = jsc.hadoopConfiguration();
    configureBigQueryOutput(
        hadoopConf,
        options.getProject(),
        options.getDataset(),
        options.getTableName(),
        options.getTableSchema());

    final Long startMinTimestamp = timestampParser.parseMillis(options.getStartMin());
    final Long stopMinTimestamp = timestampParser.parseMillis(options.getStopMin());
    final Long windowDuration = Duration.standardMinutes(options.getWindowDuration()).getMillis();

    // Run a pipeline to analyze all the data in batch.
    // First, read events from a text file and parse them.
    JavaRDD<GameActionInfo> gameEvents =
        jsc.textFile(options.getInput())
            .flatMap(new ParseEventFn())
            // Filter out data before and after the given times so that it is not included
            // in the calculations. As we collect data in batches (say, by day), the batch for
            // the day that we want to analyze could potentially include some late-arriving
            // data from the previous day. If so, we want to weed it out. Similarly, if we include
            // data from the following day (to scoop up late-arriving events from the day we're
            // analyzing), we need to weed out events that fall after the time period we want to
            // analyze.
            .filter((GameActionInfo gInfo) -> gInfo.getTimestamp() > startMinTimestamp)
            .filter((GameActionInfo gInfo) -> gInfo.getTimestamp() < stopMinTimestamp);

    JavaPairRDD<WithTimestamp<String>, Integer> hourlyTeamScores =
        gameEvents
            .mapToPair(
                event
                    -> new Tuple2<>(
                        // Extract the composite key as <team, window_start_time>
                        WithTimestamp.create(
                            event.getTeam(),
                            // Apply Fixed Window by rounding the timestamp down to the nearest
                            // multiple of the window size
                            (event.getTimestamp() / windowDuration) * windowDuration),
                        // Extract the scores as values
                        event.getScore()))
            // Compute the sum of the scores per team per window
            .reduceByKey(new SumScore());

    // Write to a BigQuery table
    JavaPairRDD<String, JsonObject> jsonPairs = hourlyTeamScores.mapToPair(convertToJson);
    jsonPairs.saveAsNewAPIHadoopDataset(hadoopConf);
  }
}
