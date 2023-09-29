package pl.kempa.saska.task4;

import static java.util.Optional.ofNullable;

import java.util.Arrays;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class Task4App {

	@Value("${app.kafka-topics.topic4}") private String topic4;

	public static void main(String[] args) {
		SpringApplication.run(Task4App.class, args);
	}

	@Bean
	public EmployeeSerializer serializer() {
		return new EmployeeSerializer();
	}

	@Bean
	public EmployeeDeserializer deserializer() {
		return new EmployeeDeserializer();
	}

	@Bean
	public KStream<String, EmployeeDTO> inputTopicStream(StreamsBuilder streamsBuilder) {
//		var serializer = new EmployeeSerializer();
//		var deserializer = new EmployeeDeserializer();
		KStream<String, EmployeeDTO> inputTopicStream =
				streamsBuilder.stream(topic4,
								Consumed.with(Serdes.String(), Serdes.serdeFrom(serializer(), deserializer())))
						.peek((k, v) -> log.info("[TOPIC4]: " + ofNullable(v).map(EmployeeDTO::toString).orElse(null)));

		return inputTopicStream;
	}

	public void testTopology(StreamsBuilder streamsBuilder, String output) {
		inputTopicStream(streamsBuilder).to(output, Produced.with(Serdes.String(), Serdes.serdeFrom(serializer(),
				deserializer())));
	}

	public void createTopology(StreamsBuilder builder) {
		final KStream<String, String> textLines = builder
				.stream("streams-plaintext-input",
						Consumed.with(Serdes.String(), Serdes.String()));

		textLines
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.groupBy((key, value) -> value)
				.count(Materialized.as("WordCount"))
				.toStream()
				.to("streams-wordcount-output",
						Produced.with(Serdes.String(), Serdes.Long()));
	}
}
