package ang.neggaw.consumers;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@Log4j2
@EnableKafka
@SpringBootApplication
public class C02_KafkaConsumerSpringApp {
    public static void main(String[] args) {
        SpringApplication.run(C02_KafkaConsumerSpringApp.class, args);
    }

    @KafkaListener(topics = "kafka-producer-spring-topic", groupId = "kafka-streams-consumer-group-id-01")
    public void start_kafka_streams_consumer_spring(ConsumerRecord<String, String> consumerRecord) {
        System.out.printf("Receiving key: '%s' with message: '%s' and topic: '%s'\n", consumerRecord.key(), consumerRecord.value(), consumerRecord.topic());
    }
}
