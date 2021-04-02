#!/bin/bash

$SPARK_HOME/bin/spark-shell --jars "$HOME/.ivy2/cache/com.typesafe.play/play-json_2.11/jars/play-json_2.11-2.7.4.jar,$HOME/.ivy2/cache/com.typesafe.play/play-functional_2.11/jars/play-functional_2.11-2.7.4.jar" $1 $2 $3
