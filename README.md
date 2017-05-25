# Code for logistic regression

## ================================

## Introduction

**logistic-regression** is written for model train through logistic regression

## Build

$ git clone https://github.com/rainness/logistic-regression.git

$ cd logistic regression

$ mvn clean compile

$ mvn -DskipTests clean package

## Quick Start

$ $HADOOP_HOME/bin/hadoop jar logistic-regression-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.github.rainness.model.SGDTrainTool \

      --input-data-path=$inputPath \
      
      --iterator-data-path=$outputPath \
      
      --combine-data-path=$combinePath \
      
      --coefficient-data-path=$coefficientPath \
      
      --learning-rate=0.001 \
      
      --iterator-num=100 \
      
      --reducer-num=50 \
      
      --feature-size=10000000


## License

Copyright 2016 Rainness Person.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
