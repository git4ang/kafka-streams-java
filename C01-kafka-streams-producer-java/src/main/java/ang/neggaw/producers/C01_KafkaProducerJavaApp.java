package ang.neggaw.producers;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.kafka.annotation.EnableKafka;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Log4j2
@EnableKafka
@SpringBootApplication
public class C01_KafkaProducerJavaApp {
    public static void main(String[] args) {
        SpringApplication.run(C01_KafkaProducerJavaApp.class, args);
    }

    @Value(value = "${app4ang.file.filename}")
    private Resource filename;

    private int counter;
    List<List<String>> stringsLinesFile = new ArrayList<>();
    String string;
    int linesNumber = 0;
    int stringsLinesNumber = 0;

    @Bean
    public void start_kafka_producer_java() {

        String BOOTSTRAP_SERVER = "localhost:9092";
        String CLIENT_ID = "kafka-streams-producer-client-id-01";

        Properties prop = new Properties();
        prop.putAll(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER,
                ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

        try (Stream<String> stream = Files.lines(Paths.get(filename.getURI()))) {
            stream
                .map(s -> s.split(" "))
                .map(Arrays::asList)
                .map(listStrings -> {
                    stringsLinesFile.add(listStrings);
                    return stringsLinesFile;
                })
                .toList();
                //.forEach(System.out::println);

            System.out.println(stringsLinesFile.size());
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {

                string = stringsLinesFile.get(linesNumber).get(stringsLinesNumber);
                stringsLinesNumber++;
                if(stringsLinesNumber == stringsLinesFile.get(linesNumber).size()) {
                    linesNumber++;
                    stringsLinesNumber = 0;
                    System.out.println("*********************** Line number: " + linesNumber + " *************************");
                }

                kafkaProducer.send(new ProducerRecord<String, String>("kafka-java-topic", "" + counter++, string), (md, ex) -> {
                    System.out.printf("Sending message: '%s' from topic: '%s' to producer. Message number: '%s'. \n", string, md.topic(), counter);
                });

            }, 1000, 1000, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}

