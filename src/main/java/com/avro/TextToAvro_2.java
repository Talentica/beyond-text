package com.avro;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.avro.AvroParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import com.Ostermiller.util.CSVParser;
/**
* The classic WordCount example modified to output Avro Pair<CharSequence,
* Integer> records instead of text.
*/
public class TextToAvro_2 extends Configured implements Tool {
public static class Map
extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, NullWritable> {
    
    String schema1;
    public void setup(Context context) throws IOException , InterruptedException{
         context.getConfiguration();
         schema1 = context.getConfiguration().get("schema2");
          
          
           
        

            }

public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
throws IOException, InterruptedException {
    //InputStream stream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8));
    
    //InputStream in = IOUtils.toInputStream(value.toString(), "UTF-8");
    
    // Schema schema=Schema.parse(stream);
     Path p = new Path(schema1);
    FileSystem fs = FileSystem.get(context.getConfiguration());
       InputStream in = fs.open(p);
    Schema schema3 = new Schema.Parser().parse(in);

    List<Field> l = schema3.getFields();
    
    System.out.print("length of schema" + l.size());
    //Schema schema3 = new Schema.Parser().parse(new File("/home/edureka/Downloads/twitter.avsc"));

    //DatumReader    <GenericRecord> datumReader    =    new    GenericDatumReader<GenericRecord>(schema3);
//    GenericRecord e1 = new GenericData.Record(schema3);
    
//    DataFileReader<GenericRecord>dataFileReader=new DataFileReader<GenericRecord>(new File("/home/Hadoop/Avro_Work/with_code_genfile/emp.avro"),    datumReader);
    //GenericRecord em=null;
    //while(dataFileReader.hasNext()){
    //    em = dataFileReader.next(em);
        System.out.print("em");
        
    
    // e1.put(0, "a");
    // e1.put(1, "b");
    // long v= 1366150681;
    // e1.put(2,v );
    
    // AvroKey<GenericRecord> key1 = new AvroKey<GenericRecord>(e1);
        String a="";
         //String d="";
      for(int i=0;i< l.size();i++){
          if(a.equals("")){
              a = key.datum().get(i).toString();
          }
          else
          {
              a = a + "," + key.datum().get(i).toString();
          }
        
      }
       
      context.write(new Text(a), NullWritable.get());
}
}


public int run(String[] args) throws Exception {
/*if (args.length != 2) {
System.err.println("Usage: AvroWordCount <input path> <output path>");
return -1;
}*/
Job job = new Job(getConf());
job.setJarByClass(TextToAvro_2.class);
job.setJobName("AvroToText_2");
// We call setOutputSchema first so we can override the configuration
// parameters it sets
String s = args[0];
Configuration conf = job.getConfiguration();
Path p = new Path(s);
FileSystem fs = FileSystem.get(conf);
InputStream in = fs.open(p);
Schema schema = new Schema.Parser().parse(in);
System.out.print("schema"+schema);
job.getConfiguration().set("schema2", s);
FileInputFormat.addInputPath(job, new Path(args[1]));
job.setInputFormatClass(AvroKeyInputFormat.class);
AvroJob.setInputKeySchema(job, schema);

job.setOutputFormatClass(TextOutputFormat.class);
TextOutputFormat.setOutputPath(job, new Path(args[2]));

/* Impala likes Parquet files to have only a single row group.
* Setting the block size to a larger value helps ensure this to
* be the case, at the expense of buffering the output of the
* entire mapper's split in memory.
*
* It would be better to set this based on the files' block size,
* using fs.getFileStatus or fs.listStatus.
*/
AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
job.setMapperClass(Map.class);
job.setNumReduceTasks(0);
return job.waitForCompletion(true) ? 0 : 1;
}
public static void main(String[] args) throws Exception {
int res =ToolRunner.run(new Configuration(), new TextToAvro_2(), args);
System.exit(res);
}
}