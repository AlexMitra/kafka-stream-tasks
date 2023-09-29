package pl.kempa.saska.task4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class Task4AppExampleTests {

	private TopologyTestDriver testDriver;
	private TestInputTopic<String, String> plainTextInput;
	private TestOutputTopic<String, Long> wordCountOutput;
	private Serde<String> stringSerde = new Serdes.StringSerde();
	private Serde<Long> longSerde = new Serdes.LongSerde();

	@BeforeEach
	public void init() {
		final Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());

		Task4App wordCountTopology = new Task4App();
		final StreamsBuilder builder = new StreamsBuilder();
		wordCountTopology.createTopology(builder);
		final Topology topology = builder.build();

		testDriver = new TopologyTestDriver(topology, props);
		plainTextInput = testDriver.createInputTopic("streams-plaintext-input", stringSerde.serializer(),
				stringSerde.serializer());
		wordCountOutput = testDriver.createOutputTopic("streams-wordcount-output", stringSerde.deserializer(),
				longSerde.deserializer());
	}

	@AfterEach
	public void tearDown() throws IOException {
		testDriver.close();
	}

	@Test
	public void testWordCountStream() {
		String text1 = "Welcome to kafka streams";
		String text2 = "Kafka streams is great";
		String text3 = "Welcome back";

		// expected output
		Map<String,Long> expected = Map.of(
				"welcome", 2L,
				"to", 1L,
				"kafka", 2L,
				"streams", 2L,
				"is", 1L,
				"great", 1L,
				"back", 1L
		);

		plainTextInput.pipeInput(null,text1);
		plainTextInput.pipeInput(null,text2);
		plainTextInput.pipeInput(null,text3);

		assertFalse(wordCountOutput.isEmpty());

		// result
		Map<String, Long> result = new HashMap<>();
		while(!wordCountOutput.isEmpty()) {
			final KeyValue<String, Long> kv = wordCountOutput.readKeyValue();
			result.put(kv.key, kv.value);
		}
		assertThat(result).containsExactlyInAnyOrderEntriesOf(expected);
		assertThat(wordCountOutput.isEmpty()).isTrue();
	}
}
