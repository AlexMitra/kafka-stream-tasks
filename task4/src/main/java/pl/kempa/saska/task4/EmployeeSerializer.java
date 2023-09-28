package pl.kempa.saska.task4;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class EmployeeSerializer implements Serializer<EmployeeDTO> {

	private ObjectMapper objectMapper;

	public EmployeeSerializer() {
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Serializer.super.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(String topic, EmployeeDTO data) {
		try {
			if (data == null) {
				return null;
			}
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error when serializing EmployeeDTO to byte[]");
		}
	}

	@Override
	public void close() {
		Serializer.super.close();
	}
}
