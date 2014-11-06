package hadoopExercise;

public class UFORecordReader {
	
	public static final int RECORD_LENGTH = 6;
	
	public static final String MISSING_VALUE = "none";
	
	public static final String RECORD_DELIMITER = "\t";
	
	private String originalRecord;
	
	private Long sightedAt;

	private Long reportedAt;
	
	private String location;

	private String shape;

	private String duration;

	private String description;

	public String getOriginalRecord() {
		
		return originalRecord;
		
	}

	public void setOriginalRecord(String originalRecord) {
		
		this.originalRecord = originalRecord;
			
	}

	public Long getSightedAt() {
		
		return sightedAt;
		
	}

	public void setSightedAt(String sightedAt) {
		
		try {
		
			this.sightedAt = new Long(sightedAt);
			
		}
		
		catch (Exception e) {
			
			this.sightedAt = new Long(0);
			
		}
		
	}

	public Long getReportedAt() {
		
		return reportedAt;
		
	}

	public void setReportedAt(String reportedAt) {
		
		try {
			
			this.reportedAt = new Long(reportedAt);
			
		}
		
		catch (Exception e) {
			
			this.reportedAt = new Long(0);
			
		}
		
	}

	public String getLocation() {
		
		return location;
		
	}

	public void setLocation(String location) {
		
		if (location == null || location.isEmpty()) {
			
			this.location = MISSING_VALUE;
			
		}
		
		else {
		
			this.location = location;
			
		}
		
	}

	public String getShape() {
		
		return shape;
		
	}

	public void setShape(String shape) {
		
		if (shape == null || shape.isEmpty()) {
			
			this.shape = MISSING_VALUE;
			
		}
		
		else {
		
			this.shape = shape.trim();
			
		}
		
	}

	public String getDuration() {
		
		return duration;
		
	}

	public void setDuration(String duration) {
		
		if (duration == null || duration.isEmpty()) {
			
			this.duration = MISSING_VALUE;
			
		}
		
		else {
		
			this.duration = duration;
			
		}
		
	}

	public String getDescription() {
		
		return description;
		
	}

	public void setDescription(String description) {
		
		if (description == null || description.isEmpty()) {
			
			this.description = MISSING_VALUE;
			
		}
		
		else {
		
			this.description = description;
			
		}
		
	}
	
	public void parse(String line) {
		
		if (line == null) {
			
			throw new IllegalArgumentException("Null record");
			
		}
		
		String[] splitRecord = line.split("\\t", -1);
		
		if (splitRecord.length != RECORD_LENGTH) {
			
			throw new IllegalArgumentException(String.format("Record not fomatted correctly, %s, not %s, fields were parsed",
					splitRecord.length, RECORD_LENGTH));
			
		}
		
		else {
			
			setOriginalRecord(line);
			
			setSightedAt(splitRecord[0]);
			setReportedAt(splitRecord[1]);
			setLocation(splitRecord[2]);
			setShape(splitRecord[3]);
			setDuration(splitRecord[4]);
			setDescription(splitRecord[5]);
			
		}
		
	}
	
	public String toString() {
		
		return getOriginalRecord();
		
	}

}
