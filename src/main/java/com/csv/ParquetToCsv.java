package com.csv;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

public class ParquetToCsv extends Configured implements Tool {
	private static final Log log = Log.getLog(ParquetToCsv.class);

	public static class CsvMapper extends
	Mapper<LongWritable, Group, NullWritable, Text> {
		List<Type> l = null;

		public void map(LongWritable key, Group value, Context context)
				throws IOException, InterruptedException {

			if (l == null) {
				String schema2 = ((ParquetInputSplit) context.getInputSplit())
						.getFileSchema();
				System.out.print("in mapper schema2" + schema2);
				MessageType schema3 = MessageTypeParser
						.parseMessageType(schema2);
				System.out.print("in mapper schema3" + schema3);
				l = schema3.getFields();
			}

			System.out.print("in mapper fields " + l);
			String line = value.toString();
			System.out.print("line" + line);
			String[] fields = line.split("\n");
			StringBuilder csv = new StringBuilder();
			boolean hasContent = false;
			int i = 0;
			Iterator<Type> it = l.iterator();
			while (it.hasNext()) {

				if (hasContent) {
					csv.append(',');
				}
				System.out.print("in while loop" + csv);
				String name = it.next().getName();
				if (fields.length > i) {
					String[] parts = fields[i].split(": ");

					if (parts[0].equals(name)) {
						boolean mustQuote = (parts[1].contains(",") || parts[1]
								.contains("'"));
						if (mustQuote) {
							csv.append('"');
						}
						csv.append(parts[1]);
						if (mustQuote) {
							csv.append('"');
						}
						hasContent = true;
						i++;
					}
				}
			}
			NullWritable outKey = NullWritable.get();

			System.out.print("outside while loop" + csv);
			context.write(outKey, new Text(csv.toString()));

		}

	}

	public int run(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];
		Path parquetFileInputPath = null;
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputPath), true);
		while (it.hasNext()) {
			FileStatus fs = it.next();
			if (fs.isFile()) {
				parquetFileInputPath = fs.getPath();
				break;
			}
		}
		if (parquetFileInputPath == null) {
			log.error("No file found for " + inputPath);
			return 1;
		}

		ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(),
				parquetFileInputPath);

		MessageType schema = readFooter.getFileMetaData().getSchema();
		job.getConfiguration().set("inputpath1", inputPath);

		System.out.print(schema);

		FileInputFormat.addInputPath(job, parquetFileInputPath);
		job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(CsvMapper.class);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ParquetToCsv(), args);
		System.exit(exitCode);
	}
}