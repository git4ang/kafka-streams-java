package ang.neggaw.consumers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class C01_KafkaConsumerJavaApp {

    public static void main(String[] args) {
        SpringApplication.run(C01_KafkaConsumerJavaApp.class, args);
    }

    @Bean
    public void start_kafka_consumer_java() {

        String BOOTSTRAP_SERVER = "localhost:9092";

        Properties prop = new Properties();
        prop.putAll(Map.of(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER,
                StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-consumer-application-id-01",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000
        ));

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stringsLines = streamsBuilder.stream("kafka-java-topic");

        KTable<String, Long> result_stringsLines = stringsLines.map((k, v) -> new KeyValue<>(k, v.toUpperCase()))
                .filter((k, v) -> v.contains("A")  || v.contains("S"))
                .groupBy((k, v) -> v)
                .count(Materialized.as("count-strings"));
//                .foreach((k, v) -> System.out.println("key: " + k + " value: " + v));

        result_stringsLines
                .toStream()
                .to("kafka-java-results-topic", Produced.with(Serdes.String(), Serdes.Long()));
//                .foreach((k, v) -> System.out.println("key: " + k + " value: " + v));

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, prop);
        kafkaStreams.start();
    }
}
