package com.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The classic WordCount example modified to output Avro Pair<CharSequence,
 * Integer> records instead of text.
 */
public class SequenceToText extends Configured implements Tool {
	
	public static class Map extends
	Mapper<NullWritable, Text, Text, NullWritable> {

		public void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(value, NullWritable.get());
		}
	}

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(SequenceToText.class);
		job.setJobName("SequenceToText");

		Configuration conf = job.getConfiguration();

		job.setMapperClass(Map.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new SequenceToText(),
				args);
		System.exit(res);
	}
}