package com.csv;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import parquet.Log;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
//import parquet.org.apache.thrift.protocol.TBinaryProtocol.Factory;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

import org.apache.hadoop.io.Text;
import parquet.hadoop.ParquetInputSplit;






public class ParquetToCsv extends Configured implements Tool {
    private static final Log log = Log.getLog(ParquetToCsv.class);
    
    //static String rawschema;
    public static class CsvMapper extends Mapper<LongWritable, Group, NullWritable, Text> {
         List<Type> l = null;
   public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
         
       if(l==null){
         String schema2= ((ParquetInputSplit)context.getInputSplit()).getFileSchema();
         System.out.print("in mapper schema2" + schema2);
         MessageType schema3 = MessageTypeParser.parseMessageType(schema2);
         System.out.print("in mapper schema3" + schema3);
         l= schema3.getFields();
       }
       
       System.out.print("in mapper fields " + l);
       String line = value.toString();
       System.out.print("line" + line);
       String[] fields = line.split("\n");
       StringBuilder csv = new StringBuilder();
       boolean hasContent = false;
       int i = 0;
       // Look for each expected column
       Iterator<Type> it = l.iterator();
       while(it.hasNext()) {
          
       if(hasContent ) {
       csv.append(',');
       }
       System.out.print("in while loop" + csv);
       String name = it.next().getName();
       if(fields.length > i) {
       String[] parts = fields[i].split(": ");
       // We assume proper order, but there may be fields missing
       if(parts[0].equals(name)) {
       boolean mustQuote = (parts[1].contains(",") || parts[1].contains("'"));
       if(mustQuote) {
       csv.append('"');
       }
       csv.append(parts[1]);
       if(mustQuote) {
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
            // all paths in HDFS
            // path to Avro schema file (.avsc)
       //     String compression_type = args.length>2?args[0]:"no compression";
            String inputPath = args[0];
            String outputPath =args[1];
            Path parquetFileInputPath = null;

            Job job = new Job(getConf());
            job.setJarByClass(getClass());
            Configuration conf = job.getConfiguration();
            RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputPath), true);
            while(it.hasNext()){
                FileStatus fs = it.next();
                if(fs.isFile()){
                    parquetFileInputPath = fs.getPath();
                    break;
                }
            }
            if(parquetFileInputPath == null) {
                log.error("No file found for " + inputPath);
                return 1;
                }

            // read in the Avro schema
           
           // Schema avroSchema = new Schema.Parser().parse(in);
            ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(), parquetFileInputPath);
            
            MessageType schema = readFooter.getFileMetaData().getSchema();
           // GroupReadSupport readSupport = new GroupReadSupport();
          //  readSupport.init(configuration, null, schema);
            job.getConfiguration().set("inputpath1", inputPath);
           
            System.out.print(schema);
            
           // job.getConfiguration().set("schema2", rawschema);
            // point to input data
            FileInputFormat.addInputPath(job, parquetFileInputPath);
            job.setInputFormatClass(ExampleInputFormat.class);
            
           
            // set the output format
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path(outputPath));
       //     ExampleOutputFormat.setSchema(job, schema);
         /*   CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
            if(compression_type.equalsIgnoreCase("snappy")) {
                codec = CompressionCodecName.SNAPPY;
                } else if(compression_type.equalsIgnoreCase("gzip")) {
                codec = CompressionCodecName.GZIP;
                }*/
          //  ExampleOutputFormat.setCompression(job, codec);
           // ExampleOutputFormat.setCompressOutput(job, true);

            // set a large block size to ensure a single row group.  see discussion
          //  ExampleOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

            job.setMapperClass(CsvMapper.class);
            job.setNumReduceTasks(0);

            return job.waitForCompletion(true) ? 0 : 1;
        }

     public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new ParquetToCsv(), args);
            System.exit(exitCode);
        }
    }