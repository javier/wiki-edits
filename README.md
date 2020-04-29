### WIKI-EDITS. Apache Flink + Apache Kafka Demo

__This code is heavily based in the monitoring wikipedia tutorial at__ https://ci.apache.org/projects/flink/flink-docs-release-1.0/quickstart/run_example_quickstart.html

It also got a lot of good ideas from https://github.com/arjunsk/flink-kinesis-data-analytics-sample  (Thank you, Arjun)


# WikipediaAnalisys

It is pretty much the original version at the flink website, but adding some configuration options

If you try to run this as a Kinesis Data Analytics application, you need to pay attention to networking as by default KDA running on a VPC doesn't have access to public internet, and you need this for connecting to the wikipedia IRC
# WikipediaToKafka

Reads the data from the wikipedia source, and streams to Kafka as JSON

The plan is to have this running outside Kinesis Data Analytics, and stream into a Kafka topic


# WikipediaAnalysisFromKafka

Reads from a Kafka Topic, aggrgeates and writes into another topic

This file can be run as a Kinesis Data Analytics application connected to a MSK cluster via a basic VPC configuration



