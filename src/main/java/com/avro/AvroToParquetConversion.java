/*Avro to Parquet conversion code */

package com.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.avro.AvroParquetOutputFormat;
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.metadata.CompressionCodecName;

public class AvroToParquetConversion extends Configured implements Tool {

	public static class AvroMapper extends
			Mapper<AvroKey<GenericRecord>, NullWritable, Void, GenericRecord> {
		@Override
		protected void map(AvroKey<GenericRecord> key, NullWritable value,
				Context context) throws IOException, InterruptedException {
			context.write(null, key.datum());
		}
	}

	public int run(String[] args) throws Exception {
		
		Path schemaPath = new Path(args[0]);
		Path inputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = fs.open(schemaPath);
		
		/* Parse in-built avro file schema */
		Schema avroSchema = new Schema.Parser().parse(in);
		System.out.println(new AvroSchemaConverter().convert(avroSchema)
				.toString());
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		
		/*Use avro to parquet output format to generate parquet file  */
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, outputPath);
		AvroParquetOutputFormat.setSchema(job, avroSchema);
		AvroParquetOutputFormat
				.setCompression(job, CompressionCodecName.SNAPPY);
		AvroParquetOutputFormat.setCompressOutput(job, true);
		/*
		 * Impala likes Parquet files to have only a single row group. Setting
		 * the block size to a larger value helps ensure this to be the case, at
		 * the expense of buffering the output of the entire mapper's split in
		 * memory.
		 * 
		 * It would be better to set this based on the files' block size, using
		 * fs.getFileStatus or fs.listStatus.
		 */
		AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
		job.setMapperClass(AvroMapper.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvroToParquetConversion(), args);

		System.exit(exitCode);
	}
}
