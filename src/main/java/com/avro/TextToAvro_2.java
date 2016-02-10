package com.avro;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.Ostermiller.util.CSVParser;
/**
* The classic WordCount example modified to output Avro Pair<CharSequence,
* Integer> records instead of text.
*/
public class TextToAvro_2 extends Configured implements Tool {
public static class Map
extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
    
    String schema1;
    public void setup(Context context) throws IOException , InterruptedException{
         context.getConfiguration();
         schema1 = context.getConfiguration().get("schema2");
          
          
           
        

            }

public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
    //InputStream stream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8));
    
    //InputStream in = IOUtils.toInputStream(value.toString(), "UTF-8");
    
    // Schema schema=Schema.parse(stream);
     Path p = new Path(schema1);
    FileSystem fs = FileSystem.get(context.getConfiguration());
       InputStream in = fs.open(p);
    Schema schema3 = new Schema.Parser().parse(in);
    //Schema schema3 = new Schema.Parser().parse(new File("/home/edureka/Downloads/twitter.avsc"));
    GenericRecord e1 = new GenericData.Record(schema3);
    
     System.out.print("genericrecord" + e1);
     String line = value.toString();
     InputStream is = new ByteArrayInputStream(line.getBytes("UTF-8"));
        CSVParser cp =null;
        String[] nextLine ;
        cp = new CSVParser(is);
        
    nextLine = cp.getLine();
        
    System.out.print("length" + nextLine.length);
        for(int i=0 ;i< nextLine.length; i++){
            if(i==2){
                
                e1.put(i, Long.valueOf(nextLine[i]));
            }
            else{
            
            System.out.print("lines" + nextLine[i]);
            e1.put(i, nextLine[i]);
            }
        }
    
    // e1.put(0, "a");
    // e1.put(1, "b");
    // long v= 1366150681;
    // e1.put(2,v );
    
     AvroKey<GenericRecord> key1 = new AvroKey<GenericRecord>(e1);
    
    context.write(key1, NullWritable.get());
}
}


public int run(String[] args) throws Exception {
/*if (args.length != 2) {
System.err.println("Usage: AvroWordCount <input path> <output path>");
return -1;
}*/
Job job = new Job(getConf());
job.setJarByClass(TextToAvro_2.class);
job.setJobName("TextToAvro_2");
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
AvroJob.setOutputKeySchema(job,schema);
AvroJob.setOutputValueSchema(job,Schema.create(Type.NULL));
//job.setOutputValueClass(NullWritable.class);
job.setMapperClass(Map.class);
job.setNumReduceTasks(0);
job.setInputFormatClass(TextInputFormat.class);
//job.setMapOutputKeyClass(Text.class);
//job.setMapOutputValueClass(IntWritable.class);
//job.setSortComparatorClass(Text.Comparator.class);
FileInputFormat.setInputPaths(job, new Path(args[1]));
job.setOutputFormatClass(AvroKeyOutputFormat.class);
AvroKeyOutputFormat.setOutputPath(job, new Path(args[2]));
job.waitForCompletion(true);
return 0;
}
public static void main(String[] args) throws Exception {
int res =
ToolRunner.run(new Configuration(), new TextToAvro_2(), args);
System.exit(res);
}
}