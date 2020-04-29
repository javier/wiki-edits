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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Map;
import java.util.Properties;


public class WikipediaAnalysis {
    static Logger LOG = LoggerFactory.getLogger(WikipediaAnalysis.class);

    public static void main(String[] args) throws Exception {
        LOG.warn("execution started XXX");

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationPropertiesMap;

        if (see instanceof LocalStreamEnvironment) {
            applicationPropertiesMap =
                    KinesisAnalyticsRuntime.getApplicationProperties(
                            WikipediaAnalysis.class
                                    .getClassLoader()
                                    .getResource("application-properties-dev.json")
                                    .getPath());
        } else {
            applicationPropertiesMap = KinesisAnalyticsRuntime.getApplicationProperties();
        }
        String kafkaSinkBrokerList = (String) applicationPropertiesMap.get("KafkaSink").get("brokerList");
        String kafkaSinkTopic = (String) applicationPropertiesMap.get("KafkaSink").get("topic");

        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
                        acc.f0 = event.getUser();
                        acc.f1 += event.getByteDiff();
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
