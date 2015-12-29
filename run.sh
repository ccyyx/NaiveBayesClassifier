#!/bin/bash
 hadoop fs -rmr Prediction/output/
 hadoop com.sun.tools.javac.Main Prediction.java
 jar cf Prediction.jar Prediction*.class
 hadoop jar Prediction.jar Prediction Prediction/input/ Prediction/output/
 rm -rf ./a
 hadoop fs -get Prediction/output/part-r-00000 ~/a
