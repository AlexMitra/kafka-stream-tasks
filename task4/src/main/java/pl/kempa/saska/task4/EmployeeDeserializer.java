package pl.kempa.saska.task4;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EmployeeDeserializer implements Deserializer<EmployeeDTO> {

	private ObjectMapper objectMapper;

	public EmployeeDeserializer() {
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Deserializer.super.configure(configs, isKey);
	}

	@Override
	public EmployeeDTO deserialize(String topic, byte[] data) {
		try {
			if (data == null) {
				return null;
			}
			return objectMapper.readValue(new String(data, "UTF-8"), EmployeeDTO.class);
		} catch (Exception e) {
			throw new SerializationException("Error when deserializing byte[] to EmployeeDTO");
		}
	}

	@Override
	public void close() {
		Deserializer.super.close();
	}
}
