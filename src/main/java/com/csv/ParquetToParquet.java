package com.csv;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;






public class ParquetToParquet extends Configured implements Tool {
    private static final Log log = Log.getLog(ParquetToParquet.class);
    
    //static String rawschema;
    public static class ParquetMapper extends Mapper<LongWritable, Group, Void, Group> {
    
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
        context.write(null,value);
        }
}
    
     public int run(String[] args) throws Exception {
            // all paths in HDFS
            // path to Avro schema file (.avsc)
            String compression_type = args.length>2?args[0]:"no compression";
            String inputPath = args[1];
            String outputPath =args[2];
            Path parquetFileInputPath = new Path(inputPath);

            Job job = new Job(getConf());
            job.setJarByClass(getClass());
            Configuration conf = job.getConfiguration();
           // RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputPath), true);
           /* while(it.hasNext()){
                FileStatus fs = it.next();
                if(fs.isFile()){
                    parquetFileInputPath = fs.getPath();
                    break;
                }
            }*/
            if(parquetFileInputPath == null) {
                log.error("No file found for " + inputPath);
                return 1;
                }

            // read in the Avro schema
           
           // Schema avroSchema = new Schema.Parser().parse(in);
            ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(), parquetFileInputPath);
            
            MessageType schema = readFooter.getFileMetaData().getSchema();
            GroupWriteSupport.setSchema(schema, getConf());
            
            System.out.print(schema);
            
           // job.getConfiguration().set("schema2", rawschema);
            // point to input data
            
            job.setInputFormatClass(ExampleInputFormat.class);
            ExampleInputFormat.addInputPath(job, parquetFileInputPath);
            // set the output format
            job.setOutputFormatClass(ExampleOutputFormat.class);
            ExampleOutputFormat.setOutputPath(job, new Path(outputPath));
            ExampleOutputFormat.setSchema(job, schema);
            CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
            if(compression_type.equalsIgnoreCase("snappy")) {
                codec = CompressionCodecName.SNAPPY;
                } else if(compression_type.equalsIgnoreCase("gzip")) {
                codec = CompressionCodecName.GZIP;
                }
            ExampleOutputFormat.setCompression(job, codec);
            ExampleOutputFormat.setCompressOutput(job, true);

            // set a large block size to ensure a single row group.  see discussion
            ExampleOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

            job.setMapperClass(ParquetMapper.class);
            job.setNumReduceTasks(0);

            return job.waitForCompletion(true) ? 0 : 1;
        }

     public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new ParquetToParquet(), args);
            System.exit(exitCode);
        }
    }
    

