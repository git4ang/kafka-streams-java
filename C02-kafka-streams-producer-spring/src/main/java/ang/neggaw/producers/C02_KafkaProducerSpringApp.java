package ang.neggaw.producers;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Log4j2
@RequiredArgsConstructor
@EnableScheduling
@EnableKafka
@SpringBootApplication
public class C02_KafkaProducerSpringApp {
    public static void main(String[] args) {
        SpringApplication.run(C02_KafkaProducerSpringApp.class, args);
    }

    private final KafkaTemplate<String, String> kafkaTemplate;
    private int counter;
    private int c = 0;
    private int r = 0;
    private final List<List<String>> stringsLines = new ArrayList<>();
    @Value(value = "${app4ang.file.filename}")
    private Resource filename;

    @Scheduled(fixedRate = 1000)
    public void start_kafka_producer_spring() {

        try (Stream<String> linesFile = Files.lines(Paths.get(filename.getURI()))) {
            linesFile
                    .map(Arrays::asList)
                    .map(stringsLine -> stringsLine.stream().map(s -> s.split(" ")).toList())
                    .map(strings -> strings
                            .stream()
                            .map(ss -> stringsLines.add(List.of(ss))).toList())
                    .toList();
//                    .forEach(System.out::println);

            if(r == stringsLines.get(c).size()) {
                r = 0;
                c++;
                System.out.println("*************************** Line: " + c + " ***************************");
            }
            String message = stringsLines.get(c).get(r);
            r++;

            kafkaTemplate.send(new ProducerRecord<>("kafka-producer-spring-topic", "" + counter++, message));
            System.out.printf("Sending message: '%s', to topic: '%s'. Message num. '%d' \n", message, "kafka-producer-spring-topic", counter);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
