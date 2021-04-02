#!/bin/bash

#sbt clean package
$SPARK_HOME/bin/spark-submit   \
    --class edu.vtc.arxiv.Main \
    --conf "spark.driver.extraClassPath=$HOME/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.7.4.jar:$HOME/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.7.4.jar" \
    ./target/scala-2.11/arxiv_2.11-1.0.jar

# Add '--master yarn' to the command line to use the cluster.
