package pl.kempa.saska;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Qualifier;
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
public class Task2App {

	public static void main(String[] args) {
		SpringApplication.run(Task2App.class, args);
	}

	@Value("${app.kafka-topics.topic2}") private String topic2;

	@Bean
	@Qualifier("inputTopicStream")
	public KStream<String, String> inputTopicStream(StreamsBuilder streamsBuilder) {
		KStream<String, String> inputTopicStream =
				streamsBuilder.stream(topic2, Consumed.with(Serdes.String(), Serdes.String()));

		return inputTopicStream;
	}

	@Bean
	@Qualifier("splitByShortLongWords")
	public Map<String, KStream<Integer, String>> splitByShortLongWords(
			@Qualifier("inputTopicStream") KStream<String, String> inputTopicStream) {
		KStream<Integer, String> wordsWithLengthStream = inputTopicStream.filterNot(((k, v) -> v == null))
				.flatMapValues(value -> List.of(value.split("\\W+")))
				.selectKey((key, value) -> value.length());
		wordsWithLengthStream.foreach((k, v) -> log.info("[TOPIC2]: " + transformKeyValues(k, v)));

		KStream<Integer, String>[] branches = wordsWithLengthStream.branch(
				(key, value) -> value.length() <= 10,
				(key, value) -> value.length() > 10
		);
		return Map.of("words-short", branches[0], "words-long", branches[1]);
	}

	@Bean
	@Qualifier("filterWordsWithLetterA")
	public Map<String, KStream<Integer, String>> filterWordsWithLetterA(
			@Qualifier("splitByShortLongWords") Map<String, KStream<Integer, String>> shortLongWordsStreamsMap) {
		return shortLongWordsStreamsMap.entrySet()
				.stream()
				.collect(Collectors.toMap(Map.Entry::getKey,
						entry -> entry.getValue().filter((k, v) -> v.toLowerCase().contains("a"))));
	}

	@Bean
	@Qualifier("mergedStreams")
	public Optional<KStream<Integer, String>> mergedStreams(
			@Qualifier("filterWordsWithLetterA") Map<String, KStream<Integer, String>> filteredStreamsMap) {
		var mergedStream = filteredStreamsMap.values().stream().reduce(KStream::merge);
		mergedStream.ifPresent(ks -> ks.foreach(
				(k, v) -> log.info("[TOPIC2 MERGED]: " + transformKeyValues(k, v))
				)
		);
		return mergedStream;
	}

	private String transformKeyValues(Integer key, String value) {
		return String.format("{ \"key\":%d, \"value\":\"%s\" }", key, value);
	}
}
