package wikiedits;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class WikipediaAnalysisFromKafka {
    static Logger LOG = LoggerFactory.getLogger(WikipediaAnalysisFromKafka.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationPropertiesMap;

        if (see instanceof LocalStreamEnvironment) {
            applicationPropertiesMap =
                    KinesisAnalyticsRuntime.getApplicationProperties(
                            WikipediaAnalysisFromKafka.class
                                    .getClassLoader()
                                    .getResource("application-properties-dev.json")
                                    .getPath());
        } else {
            applicationPropertiesMap = KinesisAnalyticsRuntime.getApplicationProperties();
        }
        String kafkaSourceBootstrapServers = (String) applicationPropertiesMap.get("KafkaSource").get("bootstrap.servers");
        String kafkaSourceZookeeperConnect = (String) applicationPropertiesMap.get("KafkaSource").get("zookeeper.connect");
        String kafkaSourceTopic = (String) applicationPropertiesMap.get("KafkaSource").get("topic");
        String kafkaSinkBrokerList = (String) applicationPropertiesMap.get("KafkaSink").get("brokerList");
        String kafkaSinkTopic = (String) applicationPropertiesMap.get("KafkaSink").get("topic");

        Properties kafkaSourceproperties = new Properties();
        kafkaSourceproperties.setProperty("bootstrap.servers", kafkaSourceBootstrapServers);
        kafkaSourceproperties.setProperty("zookeeper.connect", kafkaSourceZookeeperConnect);
        kafkaSourceproperties.setProperty("group.id", "wikimedia");

        DataStream<ObjectNode> edits = see.addSource(new FlinkKafkaConsumer08<ObjectNode>(kafkaSourceTopic, new JSONKeyValueDeserializationSchema(false), kafkaSourceproperties));

        KeyedStream<ObjectNode, String> keyedEdits = edits
                .keyBy(new KeySelector<ObjectNode, String>() {
                    @Override
                    public String getKey(ObjectNode event) {
                        return event.findValue("user").asText();
                    }
                });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<ObjectNode, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, ObjectNode event) {
                        acc.f0 = event.findValue("user").asText();
                        acc.f1 += event.findValue("byteDiff").asLong();
                        return acc;
                    }
                });


        result.print();
        result
                .map(new MapFunction<Tuple2<String,Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> tuple) {
                        LOG.info("XXXX  Topic {} and list {}", kafkaSinkTopic, kafkaSinkBrokerList);
                        LOG.info("result: {}", tuple.toString());
                        return tuple.toString();
                    }
                })
                .addSink(new FlinkKafkaProducer08<>(kafkaSinkBrokerList, kafkaSinkTopic, new SimpleStringSchema()));

        see.execute();
    }
}
