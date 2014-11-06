package hadoopExercise;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class UFORecordReaderTest {
	
	public static final String TEST_FILE_PATH = "src/test/resources/test.tsv";
	
	public static final String EXPECTED_RECORD_1 =
			"19951010\t19951011\t Milwaukee, WI\tcone\t2 min.\tDescription 1";
	
	public static final String EXPECTED_RECORD_2 =
			"R\t19950103\t Shelton, WA\t\t\tDescription 2";
	
	private ArrayList<String> records;

	@Before
	public void readFile() {
		
		try {
			
			BufferedReader reader =
					new BufferedReader(new FileReader(TEST_FILE_PATH));
			
			records = new ArrayList<String>();
			
			String current = reader.readLine();
			
			while (current != null) {
				
				records.add(current);
				
				current = reader.readLine();
				
			}
			
			reader.close();
			
		}

		catch (FileNotFoundException e) {
			
			e.printStackTrace();
			
		}

		catch (IOException e) {
			
			e.printStackTrace();
			
		}
		

	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testParseInvalidRecord() {
		
		UFORecordReader reader = new UFORecordReader();
		
		reader.parse(records.get(0));
		
	}
	
	@Test
	public void testParseValidRecord() {
		
		UFORecordReader reader = new UFORecordReader();
		
		reader.parse(records.get(1));
		
		assertEquals(reader.getSightedAt(), new Long(19951010));
		assertEquals(reader.getReportedAt(), new Long(19951011));
		assertEquals(reader.getLocation(), " Milwaukee, WI");
		assertEquals(reader.getShape(), "cone");
		assertEquals(reader.getDuration(), "2 min.");
		assertEquals(reader.getDescription(), "Description 1");
		
	}
	
	@Test
	public void testParseValidRecordSubstitution() {
		
		UFORecordReader reader = new UFORecordReader();
		
		reader.parse(records.get(2));
		
		assertEquals(reader.getSightedAt(), new Long(0));
		assertEquals(reader.getShape(), UFORecordReader.MISSING_VALUE);
		
	}
	
	@Test
	public void testToString() {

		UFORecordReader reader = new UFORecordReader();
		
		reader.parse(records.get(1));
		
		assertEquals(reader.toString(), EXPECTED_RECORD_1);
		
		reader.parse(records.get(2));
		
		assertEquals(reader.toString(), EXPECTED_RECORD_2);
		
	}
	
}
