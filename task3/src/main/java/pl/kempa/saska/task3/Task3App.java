package pl.kempa.saska.task3;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

import java.time.Duration;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
@Slf4j
public class Task3App {


	@Value("${app.kafka-topics.topic3-1}") private String topic3_1;

	@Value("${app.kafka-topics.topic3-2}") private String topic3_2;

	public static void main(String[] args) {
		SpringApplication.run(Task3App.class, args);
	}

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration kStreamsConfigs() {
		return new KafkaStreamsConfiguration(Map.of(
				APPLICATION_ID_CONFIG, "kafka-streams-learning-task-3",
				BOOTSTRAP_SERVERS_CONFIG, "localhost:8097",
				DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName(),
				DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
				DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
		));
	}

	@Bean
	@Qualifier("leftStream")
	public KStream<Integer, String> leftStream(StreamsBuilder streamsBuilder) {
		KStream<String, String> topic31Stream =
				streamsBuilder.stream(topic3_1, Consumed.with(Serdes.String(), Serdes.String()));
		KStream<Integer, String> leftStream = transformToPairsWithNumberKey(topic31Stream);
		leftStream.foreach((k, v) -> log.info("[TOPIC3-1]: " + transformKeyValues(k, v)));
		return leftStream;
	}

	@Bean
	@Qualifier("rightStream")
	public KStream<Integer, String> rightStream(StreamsBuilder streamsBuilder) {
		KStream<String, String> topic32Stream =
				streamsBuilder.stream(topic3_2, Consumed.with(Serdes.String(), Serdes.String()));
		KStream<Integer, String> rightStream = transformToPairsWithNumberKey(topic32Stream);
		rightStream.foreach((k, v) -> log.info("[TOPIC3-2]: " + transformKeyValues(k, v)));
		return rightStream;
	}

	@Bean
	public KStream<Integer, String> joinStream(
			@Qualifier("leftStream") KStream<Integer, String> leftStream,
			@Qualifier("rightStream") KStream<Integer, String> rightStream) {

		ValueJoiner<String, String, String> valueJoiner =
				(leftValue, rightValue) -> leftValue + "--" + rightValue;
		return leftStream.join(rightStream,
						valueJoiner,
						JoinWindows.of(Duration.ofSeconds(60)))
				.peek((k, v) -> log.info("[TASK 3 JOIN]: " + transformKeyValues(k, v)));
	}

	private KStream<Integer, String> transformToPairsWithNumberKey(KStream<String, String> stream) {
		return stream
				.filterNot((k, v) -> v == null)
				.filter((k, v) -> v.contains(":"))
				.selectKey((key, value) -> StringUtils.substringBefore(value, ":"))
				.map((k, v) -> KeyValue.pair(Integer.valueOf(k), v));
	}

	private String transformKeyValues(Integer key, String value) {
		return String.format("{ \"key\":%d, \"value\":\"%s\" }", key, value);
	}
}
