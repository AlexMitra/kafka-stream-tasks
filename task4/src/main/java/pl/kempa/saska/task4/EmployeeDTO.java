package pl.kempa.saska.task4;

public record EmployeeDTO(String name, String company, String position, Integer experience) {
	@Override
	public String toString() {
		return String.format("""
            {
                name : '%s',
                company : '%s',
                position : '%s',
                experience : '%d'
            }
            """,
				name,
				company,
				position,
				experience
		);
	}
}
