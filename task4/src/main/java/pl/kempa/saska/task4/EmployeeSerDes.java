package pl.kempa.saska.task4;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class EmployeeSerDes {
	private EmployeeSerDes() {
	}

	public static Serde<EmployeeDTO> BookSold() {
		EmployeeSerializer serializer = new EmployeeSerializer();
		EmployeeDeserializer deserializer = new EmployeeDeserializer();
		return Serdes.serdeFrom(serializer, deserializer);
	}
}
