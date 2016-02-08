package com.avro;

import java.io.IOException;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
* The classic WordCount example modified to output Avro Pair<CharSequence,
* Integer> records instead of text.
*/
public class TextToAvro extends Configured implements Tool {
public static class Map
extends Mapper<LongWritable, Text, AvroKey<CharSequence>, NullWritable> {

public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {


    context.write(new AvroKey<CharSequence>(value.toString()), NullWritable.get());
}
}


public int run(String[] args) throws Exception {
if (args.length != 2) {
System.err.println("Usage: AvroWordCount <input path> <output path>");
return -1;
}
Job job = new Job(getConf());
job.setJarByClass(TextToAvro.class);
job.setJobName("wordcount");
// We call setOutputSchema first so we can override the configuration
// parameters it sets
AvroJob.setOutputKeySchema(job,Schema.create(Type.STRING));
AvroJob.setOutputValueSchema(job,Schema.create(Type.NULL));
//job.setOutputValueClass(NullWritable.class);
job.setMapperClass(Map.class);
job.setNumReduceTasks(0);
job.setInputFormatClass(TextInputFormat.class);
//job.setMapOutputKeyClass(Text.class);
//job.setMapOutputValueClass(IntWritable.class);
//job.setSortComparatorClass(Text.Comparator.class);
FileInputFormat.setInputPaths(job, new Path(args[0]));
job.setOutputFormatClass(AvroKeyOutputFormat.class);
AvroKeyOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
return 0;
}
public static void main(String[] args) throws Exception {
int res =
ToolRunner.run(new Configuration(), new TextToAvro(), args);
System.exit(res);
}
}