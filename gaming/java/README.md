This code is intended to be used to contrast how the Spark Java solution to
a problem differs from the Dataflow solution to a problem.

Before diving in here, you likely want to read [Dataflow/Beam & Spark:
A Programming Model Comparison](https://cloud.google.com/dataflow/blog/dataflow-beam-and-spark-comparison).

The equivalent Dataflow code lives in [GoogleCloudPlatform/DataflowJavaSDK-examples](http://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/tree/master/src/main/java8/com/google/cloud/dataflow/examples/complete/game) and is documented in [Mobile Gaming Pipeline Examples](https://cloud.google.com/dataflow/examples/gaming-example).

You will want a Dataproc cluster to run these samples on. This command line
will stand up a simple 5 worker cluster with all the appropriate scopes
needed to communicate with other Cloud Platform services:

    $ gcloud dataproc clusters create spark-demo \
    --scopes cloud-platform \
    --worker-machine-type n1-standard-4 \
    --num-workers 5

You'll need to install maven. To install maven on a debian based
workstation, run:

    $ sudo apt-get install maven

Save the name of your cluster in $DATAPROC_CLUSTER, for example:

    $ export DATAPROC_CLUSTER=your-cluster-name

Create a Cloud Storage bucket to use for input and output of Dataproc jobs and
save the name in $DATAPROC_BUCKET.

    $ gsutil mb gs://your-bucket-name
    $ export DATAPROC_BUCKET=gs://your-bucket-name

To build all the samples, run:

    $ mvn package

This will produce target/game-1.0-SNAPSHOT-jar-with-dependencies.jar which
contains all the samples and their dependencies.
See below for instructions on how to run each sample individually.

## UserScore

### Running on Dataproc

You can run your job on Dataproc like so:

    $ gcloud dataproc jobs submit spark \
        --cluster $DATAPROC_CLUSTER \
        --class com.google.cloud.sparkdemo.UserScore \
        --jar target/game-1.0-SNAPSHOT-jar-with-dependencies.jar \
        --input gs://dataflow-samples/game/gaming_data*.csv \
        --tableName Daily --dataset spark_game_stats

## HourlyTeamScore

### Running on Dataproc

You can run the job on Dataproc as follows:

    $ gcloud dataproc jobs submit spark \
        --cluster $DATAPROC_CLUSTER \
        --class com.google.cloud.sparkdemo.HourlyTeamScore \
        --jar target/game-1.0-SNAPSHOT-jar-with-dependencies.jar \
        --input gs://dataflow-samples/game/gaming_data*.csv \
        --tableName Hourly --dataset spark_game_stats

## LeaderBoard

### Running Locally

You can run locally using spark-submit like so:

    $ spark-submit --class "com.google.cloud.sparkdemo.LeaderBoard" \
        --master local[4] target/game-1.0-SNAPSHOT-jar-with-dependencies.jar \
        --tableName leaderboard --dataset spark_game_stats \
        --topic gaming

### Running on Dataproc

You can run also the job on Dataproc as follows:

    $ gcloud dataproc jobs submit spark \
        --cluster $DATAPROC_CLUSTER \
        --properties spark.streaming.receiver.writeAheadLog.enabled=true,spark.executor.memory=4g,spark.executor.instances=10 \
        --class com.google.cloud.sparkdemo.LeaderBoard \
        --jar target/game-1.0-SNAPSHOT-jar-with-dependencies.jar \
        --tableName leaderboard --dataset spark_game_stats \
        --topic gaming


## GameStats

### Running Locally

You can run locally using spark-submit like so:

    $ spark-submit --class "com.google.cloud.sparkdemo.GameStats" \
        --master local[4] target/game-1.0-SNAPSHOT-jar-with-dependencies.jar \
        --tableName gamestats --dataset spark_game_stats \
        --topic gaming

### Running on Dataproc

You can run also the job on Dataproc as follows:

    $ gcloud dataproc jobs submit spark \
        --cluster $DATAPROC_CLUSTER \
        --properties spark.streaming.receiver.writeAheadLog.enabled=true,spark.executor.memory=4g,spark.executor.instances=10 \
        --class com.google.cloud.sparkdemo.GameStats \
        --jar target/game-1.0-SNAPSHOT-jar-with-dependencies.jar \
        --tableName gamestats --dataset spark_game_stats \
        --topic gaming
