package pl.kempa.saska.task1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@Configuration
@EnableKafka
@EnableKafkaStreams
public class Task1App {

	public static void main(String[] args) {
		SpringApplication.run(Task1App.class, args);
	}

}
