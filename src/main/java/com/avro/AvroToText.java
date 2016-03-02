package com.avro;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The classic WordCount example modified to output Avro Pair<CharSequence,
 * Integer> records instead of text.
 */
public class AvroToText extends Configured implements Tool {
	public static class Map extends
			Mapper<AvroKey<GenericRecord>, NullWritable, Text, NullWritable> {

		String schema1;

		public void setup(Context context) throws IOException,
				InterruptedException {
			context.getConfiguration();
			schema1 = context.getConfiguration().get("schema2");

		}

		public void map(AvroKey<GenericRecord> key, NullWritable value,
				Context context) throws IOException, InterruptedException {

			GenericRecord d = key.datum();
			Schema s = d.getSchema();
			String a = "";
			List<Field> l = s.getFields();

			for (int i = 0; i < l.size(); i++) {
				if (a.equals("")) {
					a = key.datum().get(i).toString();
				} else {
					a = a + "," + key.datum().get(i).toString();
				}

			}
			context.write(new Text(a), NullWritable.get());
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(AvroToText.class);
		job.setJobName("AvroToText");
		String s = args[0];
		Configuration conf = job.getConfiguration();
		Path p = new Path(s);
		FileSystem fs = FileSystem.get(conf);
		InputStream in = fs.open(p);
		Schema schema = new Schema.Parser().parse(in);
		System.out.print("schema" + schema);
		job.getConfiguration().set("schema2", s);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job, schema);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AvroToText(), args);
		System.exit(res);
	}
}