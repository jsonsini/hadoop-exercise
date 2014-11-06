package hadoopExercise;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class YearStateCount extends Configured implements Tool {
	
	public static final String INPUT_PATH =
			"/user/cloudera/input/ufo_awesome.tsv";

	public static final String OUTPUT_PATH =
			"/user/cloudera/yearstatecount/output";

	public static class YearStateCountMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, IntWritable> {

		private static enum Counters {
			RECORDS
		}

		public static final Set<String> STATES;
		
		static {
			STATES = new HashSet<String>();
			STATES.add("AL");
			STATES.add("AK");
			STATES.add("AZ");
			STATES.add("AR");
			STATES.add("CA");
			STATES.add("CO");
			STATES.add("CT");
			STATES.add("DE");
			STATES.add("FL");
			STATES.add("GA");
			STATES.add("HI");
			STATES.add("ID");
			STATES.add("IL");
			STATES.add("IN");
			STATES.add("IA");
			STATES.add("KS");
			STATES.add("KY");
			STATES.add("LA");
			STATES.add("ME");
			STATES.add("MD");
			STATES.add("MA");
			STATES.add("MI");
			STATES.add("MN");
			STATES.add("MS");
			STATES.add("MO");
			STATES.add("MT");
			STATES.add("NE");
			STATES.add("NV");
			STATES.add("NH");
			STATES.add("NJ");
			STATES.add("NM");
			STATES.add("NY");
			STATES.add("NC");
			STATES.add("ND");
			STATES.add("OH");
			STATES.add("OK");
			STATES.add("OR");
			STATES.add("PA");
			STATES.add("RI");
			STATES.add("SC");
			STATES.add("SD");
			STATES.add("TN");
			STATES.add("TX");
			STATES.add("UT");
			STATES.add("VT");
			STATES.add("VA");
			STATES.add("WA");
			STATES.add("WV");
			STATES.add("WI");
			STATES.add("WY");
		}

		private final static IntWritable ONE = new IntWritable(1);
		
		private static UFORecordReader reader = new UFORecordReader();

		private long numRecords = 0;
		
		private String inputFile;

		public void configure(JobConf job) {
			
			inputFile = job.get("map.input.file");

		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			try {
			
				reader.parse(value.toString());
				
				String reportedDate = Long.toString(reader.getReportedAt());
				
				String year = reportedDate.equals("0") ?
						UFORecordReader.MISSING_VALUE :
						reportedDate.substring(0, 4);
				
				String state = reader.getLocation().split(",")[1].trim();
				
				if (STATES.contains(state)) {
					
					output.collect(new Text(state + " " + year), ONE);
					
				}
				
				reporter.incrCounter(Counters.RECORDS, 1);

				if ((++numRecords % 100) == 0) {
					
					reporter.setStatus("Finished processing " + numRecords +
							" records " + "from the input file: " + inputFile);
					
				}
				
			}
			
			catch (Exception e) {
				
				reporter.setStatus("Invalid record: " + value.toString());
				
			}
			
		}
		
	}

	public static class YearStateCountReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			int sum = 0;
			
			while (values.hasNext()) {
				
				sum += values.next().get();
				
			}
			
			output.collect(key, new IntWritable(sum));
			
		}
		
	}

	public int run(String[] args) throws Exception {
		
		JobConf conf = new JobConf(getConf(), YearStateCount.class);
		conf.setJobName("yearstatecount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(YearStateCountMapper.class);
		conf.setCombinerClass(YearStateCountReducer.class);
		conf.setReducerClass(YearStateCountReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(conf, new Path(OUTPUT_PATH));

		JobClient.runJob(conf);
		
		return 0;
		
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new YearStateCount(),
				args);
		
		System.exit(res);
		
	}

}