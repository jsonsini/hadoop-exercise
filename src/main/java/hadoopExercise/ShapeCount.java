package hadoopExercise;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShapeCount extends Configured implements Tool {

	public static final String INPUT_PATH =
			"/user/cloudera/input/ufo_awesome.tsv";

	public static final String INTERMEDIATE_OUTPUT_PATH =
			"/user/cloudera/shapecount/intermediate/output";

	public static final String FINAL_OUTPUT_PATH =
			"/user/cloudera/shapecount/final/output";

	public static final int NUM_RECORDS_OUTPUT = 10;

	public static class ShapeCountIntermediateMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, IntWritable> {

		private static enum Counters {
			RECORDS
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
				
				Text shape = new Text(reader.getShape());
				
				output.collect(shape, ONE);
					
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

	public static class ShapeCountIntermediateReducer extends MapReduceBase
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

	public static class ShapeCountFinalMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, NullWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			
			output.collect(value, NullWritable.get());
					
		}
		
	}

	public static class ShapeCountFinalReducer extends MapReduceBase
			implements Reducer<Text, NullWritable, Text, NullWritable> {
		
		private static enum Counters {
			RECORDS
		}

		private long numRecords = 0;
		
		public void reduce(Text key, Iterator<NullWritable> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			
			String[] splitKey = key.toString().split("\t");
			
			reporter.incrCounter(Counters.RECORDS, 1);

			if (++numRecords <= NUM_RECORDS_OUTPUT) {
				
				output.collect(new Text(splitKey[1] + "\t" + splitKey[0]),
						NullWritable.get());
				
			}
			
		}
		
	}
	
	public static class ShapeCountFinalPartitioner
			implements Partitioner<Text, NullWritable> {
		
		public void configure(JobConf arg0) {
			
		}
		
		public int getPartition(Text key, NullWritable value,
				int numPartitions) {
			
			return (int) Long.parseLong(key.toString().split("\t")[1]) %
					numPartitions;
			
		}

	}
	
	public static class ShapeCountFinalComparator extends WritableComparator {
		
		protected ShapeCountFinalComparator() {
			
			super(Text.class, true);
			
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			
			Long count1 = Long.parseLong(t1.toString().split("\t")[1]);
			Long count2 = Long.parseLong(t2.toString().split("\t")[1]);
			
			return -1 * count1.compareTo(count2);
			
		}
		
	}
	
	public static class ShapeCountFinalGroupComparator
			extends WritableComparator {
		
		protected ShapeCountFinalGroupComparator() {
			
			super(Text.class, true);
			
		}
		
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			Text t1 = (Text) w1;
			Text t2 = (Text) w2;
			
			String shape1 = t1.toString().split("\t")[0];
			String shape2 = t2.toString().split("\t")[0];
			
			return shape1.compareTo(shape2);
			
		}
		
	}

	public int run(String[] args) throws Exception {
		
		JobConf conf = new JobConf(getConf(), ShapeCount.class);
		conf.setJobName("shapecountintermediate");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(ShapeCountIntermediateMapper.class);
		conf.setReducerClass(ShapeCountIntermediateReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(conf,
				new Path(INTERMEDIATE_OUTPUT_PATH));

		JobClient.runJob(conf);
		
		conf = new JobConf(getConf(), ShapeCount.class);
		conf.setJobName("shapecountfinal");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(ShapeCountFinalMapper.class);
		conf.setPartitionerClass(ShapeCountFinalPartitioner.class);
		conf.setOutputKeyComparatorClass(ShapeCountFinalComparator.class);
		conf.setOutputValueGroupingComparator(ShapeCountFinalGroupComparator.class);
		conf.setReducerClass(ShapeCountFinalReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(INTERMEDIATE_OUTPUT_PATH,
				"part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path(FINAL_OUTPUT_PATH));

		JobClient.runJob(conf);
		
		return 0;
		
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new ShapeCount(), args);
		
		System.exit(res);
		
	}

}