#!/bin/bash

#A=/root/NaiveBayesClassifier/blogsgender-female-100/blogsgender-female-train
C=P
B=Prediction

 hadoop fs -rmr $C/output/
 #hadoop fs -mkdir $C
 hadoop com.sun.tools.javac.Main $B.java
 jar cf $B.jar $B*.class
 hadoop jar $B.jar $B $C/input/ $C/output
 #hadoop fs -cat $C/output/*
 rm -rf ~/output/
 hadoop fs -get P/output ~

# C=CT
# B=CountryTrain

 # hadoop fs -rmr $C/output/
 # #hadoop fs -mkdir $C
 # hadoop com.sun.tools.javac.Main $B.java
 # jar cf $B.jar $B*.class
 # hadoop jar $B.jar $B $C/input/ $C/output
 # hadoop fs -cat $C/output/*