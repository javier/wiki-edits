# WIKI-EDITS. Apache Flink + Apache Kafka Demo

__This code is heavily based in the monitoring wikipedia tutorial at__ https://ci.apache.org/projects/flink/flink-docs-release-1.0/quickstart/run_example_quickstart.html

It also got a lot of good ideas from https://github.com/arjunsk/flink-kinesis-data-analytics-sample  (Thank you, Arjun)

There is a video (in Spanish) where you can watch an intro to Kafka and Flink, and a 20-minutes demo of this code https://www.youtube.com/watch?v=46ZISaEeCWs&feature=youtu.be

## Requirements

You are going to need Java 8 and Maven to run this project. Also, you need a network that can connect to IRC default port (6667). Some corporate networks block access, so be aware!

You also need to have a Kafka cluster. It might be local or it might be a Kafka running on a cloud provider. I have tried this demo both in localhost and with Amazon Managed Streaming for Kafka

## Configuration

Under resources you will find a config file with the parameters to read/write from/to Kafka. Change accordingly

Notice depending on your configuration you might need to create the Kafka topics before starting the java apps


## The files
 
### WikipediaAnalisys

It is pretty much the original version at the flink website, but adding some configuration options

If you try to run this as a Kinesis Data Analytics application, you need to pay attention to networking as by default KDA running on a VPC doesn't have access to public internet, and you need this for connecting to the wikipedia IRC

This file reads directly from the Wikimedia log change IRC channel and writes into the "KafkaSink.topic" from the configuration.

### WikipediaToKafka

Reads the data from the wikipedia source, and streams to Kafka as JSON

The plan is to have this running outside Kinesis Data Analytics, and stream into a Kafka topic

This file reads directly from the Wikimedia log change IRC channel and writes into the "KafkaSink.raw_topic" from the configuration.


### WikipediaAnalysisFromKafka

Reads from a Kafka Topic, aggrgeates and writes into another topic

This file can be run as a Kinesis Data Analytics application connected to a MSK cluster via a basic VPC configuration

This file reads directly from the "KafkaSource.topic" and writes into the "KafkaSink.topic" from the configuration.

### Executing the programs

You can just execute from your IDE, or use the mvn command line. 

*Important*: Unless you are running on Kinesis Data Analytics, you need to activate the maven profile "add-dependencies-for-IDEA".
If you don't do this, you will get an error that the source cannot be found. This is because to run in the cloud dependencies are provided, but to run locally dependencies are provided. Just use the profile and you will be good

To execute any of the files from the command line, just do this

```
mvn clean package
mvn exec:java -Dexec.mainClass=wikiedits.WikipediaToKafka -P add-dependencies-for-IDEA
```

### Executing on Kinesis Data Analytics

mvn install with the detault profile, and just upload the target jar as usual to KDA and set the configuration values


