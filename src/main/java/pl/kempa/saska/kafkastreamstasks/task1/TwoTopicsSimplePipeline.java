package pl.kempa.saska.kafkastreamstasks.task1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TwoTopicsSimplePipeline {

	@Value("${app.kafka-topics.topic1-1}")
	private String topic1_1;

	@Value("${app.kafka-topics.topic1-2}")
	private String topic1_2;

	@Autowired
	public void buildTwoTopicsSimplePipeline(StreamsBuilder streamsBuilder) {
		KStream<String, String> inputTopicStream = streamsBuilder
				.stream(topic1_1, Consumed.with(Serdes.String(), Serdes.String()));

		inputTopicStream
				.peek((k, v) -> log.info("[TOPIC1-1]: " + v))
				.to(topic1_2, Produced.with(Serdes.String(), Serdes.String()));

		streamsBuilder
				.stream(topic1_2, Consumed.with(Serdes.String(), Serdes.String()))
				.foreach((k, v) -> log.info("[TOPIC1-2]: " + v));
	}
}
