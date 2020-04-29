package wikiedits;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class WikipediaToKafka {
    static Logger LOG = LoggerFactory.getLogger(WikipediaToKafka.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationPropertiesMap;

        if (see instanceof LocalStreamEnvironment) {
            applicationPropertiesMap =
                    KinesisAnalyticsRuntime.getApplicationProperties(
                            WikipediaToKafka.class
                                    .getClassLoader()
                                    .getResource("application-properties-dev.json")
                                    .getPath());
        } else {
            applicationPropertiesMap = KinesisAnalyticsRuntime.getApplicationProperties();
        }
        String kafkaSinkBrokerList = (String) applicationPropertiesMap.get("KafkaSink").get("brokerList");
        String kafkaSinkTopic = (String) applicationPropertiesMap.get("KafkaSink").get("raw-topic");

        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        edits.print();
        edits.map(new MapFunction<WikipediaEditEvent, String>() {
                    @Override
                    public String map(WikipediaEditEvent event) {
                        return new JSONObject()
                            .put("user", event.getUser())
                            .put("byteDiff", event.getByteDiff())
                            .put("title", event.getTitle())
                            .toString();
                    }
                })
                .addSink(new FlinkKafkaProducer08<>(kafkaSinkBrokerList, kafkaSinkTopic, new SimpleStringSchema()));

        see.execute();
    }
}
