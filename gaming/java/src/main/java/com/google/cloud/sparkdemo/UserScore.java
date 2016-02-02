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

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat;
import com.google.gson.JsonObject;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.Serializable;
import java.lang.Iterable;
import java.util.Collections;

public class UserScore {

  /**
   * Class to hold info about a game event.
   */
  static class GameActionInfo implements Serializable {
    private String user;
    private String team;
    private Integer score;
    private Long timestamp;

    GameActionInfo(String user, String team, Integer score, Long timestamp) {
      this.user = user;
      this.team = team;
      this.score = score;
      this.timestamp = timestamp;
    }

    public String getUser() {
      return user;
    }

    public String getTeam() {
      return team;
    }

    public Integer getScore() {
      return score;
    }

    public Long getTimestamp() {
      return timestamp;
    }

    public String toString() {
      return "(" + getUser() + ", " + getTeam() + ", " + getTimestamp() + ", " + getScore() + ")";
    }
  }

  public static class ParseEventFn implements FlatMapFunction<String, GameActionInfo> {
    public Iterable<GameActionInfo> call(String line) {
      String[] components = line.split(",");
      try {
        String user = components[0].trim();
        String team = components[1].trim();
        Integer score = Integer.parseInt(components[2].trim());
        Long timestamp = Long.parseLong(components[3].trim());

        return Collections.singletonList(new GameActionInfo(user, team, score, timestamp));
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        return Collections.emptyList();
      }
    }
  }

  /**
   * Extract User and Score from GameActionInfo records.
   */
  public static class ExtractUserScore implements PairFunction<GameActionInfo, String, Integer> {
    @Override
    public Tuple2<String, Integer> call(GameActionInfo info) {
      return new Tuple2<>(info.getUser(), info.getScore());
    }
  }

  /**
   *Sums score pairs -- where the 'entity' will be either a team or a user.
   **/
  public static class SumScore implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) {
      return i1 + i2;
    }
  }

  // Helper to convert (user, score) tuples to JSON objects, which is what we
  // need to output them to BigQuery.
  private static PairFunction<Tuple2<String, Integer>, String, JsonObject> convertToJson =
      (kv) -> {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("user", kv._1());
        jsonObject.addProperty("total_score", kv._2());
        return new Tuple2<String, JsonObject>(null, jsonObject);
      };

  public static void configureBigQueryOutput(
      Configuration hadoopConf,
      String projectId,
      String dataset,
      String tableName,
      String tableSchema)
      throws Exception {
    // Set the job-level projectId.
    hadoopConf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
    // Configure output for BigQuery access.
    BigQueryConfiguration.configureBigQueryOutput(
        hadoopConf, dataset + "." + tableName, tableSchema);
    // Set BigQueryOutputFormat as the output format
    hadoopConf.set("mapreduce.job.outputformat.class", BigQueryOutputFormat.class.getName());
  }

  // A simple helper function to handle commandline options.
  public static class UserScoreOptions {
    protected Options options;
    private CommandLine cmd;
    private CommandLineParser cmdParser;

    public UserScoreOptions() {
      //comandline parameter parsing
      options = new Options();
      options.addOption("input", true, "Path to the input file containing game data");
      options.addOption("tableName", true, "Prefix used for the BigQuery table names");
      options.addOption("dataset", true, "BigQuery Dataset to write tables to");
      options.addOption("project", true, "Project ID used to write to Bigquery");
    }

    public void parse(String[] args) {
      cmdParser = new PosixParser();
      try {
        cmd = cmdParser.parse(options, args);

      } catch (ParseException exp) {
        System.err.println("Commandline parsing failed. Reason: " + exp.getMessage());
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("", options);
        System.exit(0);
      }
    }

    protected String getOptionWithDefaultValue(String opt, String defaultVal) {
      if (cmd.getOptionValue(opt) != null) {
        return cmd.getOptionValue(opt);
      } else {
        return defaultVal;
      }
    }

    public String getInput() {
      return getOptionWithDefaultValue("input", "gs://dataflow-samples/game/gaming_data*.csv");
    }

    public String getProject() {
      return getOptionWithDefaultValue("project", "cloud-dataflow-demo");
    }

    public String getDataset() {
      return getOptionWithDefaultValue("dataset", "spark_game_stats");
    }

    public String getTableName() {
      return getOptionWithDefaultValue("tableName", "Daily");
    }

    public String getTableSchema() {
      return "[{'name': 'user','type': 'STRING'},{'name': 'total_score','type': 'INTEGER'}]";
    }
  }

  /**
   * Run a batch pipeline.
   **/
  public static void main(String[] args) throws Exception {
    UserScoreOptions options = new UserScoreOptions();
    options.parse(args);

    SparkConf sc = new SparkConf().setAppName("UserScore");
    JavaSparkContext jsc = new JavaSparkContext(sc);

    Configuration hadoopConf = jsc.hadoopConfiguration();
    configureBigQueryOutput(
        hadoopConf,
        options.getProject(),
        options.getDataset(),
        options.getTableName(),
        options.getTableSchema());

    // Run a pipeline to analyze all the data in batch.
    JavaRDD<GameActionInfo> records =
        jsc.textFile(options.getInput())
            // First, read events from a text file and parse them.
            .flatMap(new ParseEventFn());

    // Extract username/score pairs from the event stream.
    JavaPairRDD<String, Integer> pairs = records.mapToPair(new ExtractUserScore());

    // Find the summed score per user, and output the result.
    JavaPairRDD<String, Integer> userScores = pairs.reduceByKey(new SumScore());

    // Write to a BigQuery table
    JavaPairRDD<String, JsonObject> jsonPairs = userScores.mapToPair(convertToJson);
    jsonPairs.saveAsNewAPIHadoopDataset(hadoopConf);
  }
}
