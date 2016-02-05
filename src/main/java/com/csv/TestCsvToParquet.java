package com.csv;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.Ostermiller.util.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
//import parquet.org.apache.thrift.protocol.TBinaryProtocol.Factory;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import org.apache.hadoop.io.Text;







public class TestCsvToParquet extends Configured implements Tool {
    
    //static String rawschema;
    public static class ParquetMapper extends Mapper<LongWritable, Text, Void, Group> {
        String schema1;
        public void setup(Context context) throws IOException , InterruptedException{
        
             schema1 = context.getConfiguration().get("schema2");

                }
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.print("rawschema" + schema1);
        String line = value.toString();
        InputStream is = new ByteArrayInputStream(line.getBytes("UTF-8"));
        CSVParser cp =null;
        String[] nextLine ;
        cp = new CSVParser(is);
        int iterator =0;
                
        SimpleGroupFactory sgf = new SimpleGroupFactory(MessageTypeParser.parseMessageType(schema1));
        Group group = sgf.newGroup();
        nextLine = cp.getLine();
        GroupType type  = group.getType();
        List<String> l = new ArrayList<String>();
         for(parquet.schema.Type t : type.getFields()){
             l.add(t.getName());
         }
        
        for(int i=0 ; i <l.size() ; i++){
        group.append(l.get(i),nextLine[i] );
        }
        System.out.print("value" + value);
        System.out.print("gp" + group);
        
    
        context.write(null,group);
        
        }

}
    
     public int run(String[] args) throws Exception {
            // all paths in HDFS
            // path to Avro schema file (.avsc)
            String schemaPath = args[0];
            Path inputPath = new Path(args[1]);
            Path outputPath = new Path(args[2]);

            Job job = new Job(getConf());
            job.setJarByClass(getClass());
            Configuration conf = job.getConfiguration();
            File filep = new File(schemaPath);
            System.out.print("filename " + schemaPath + "absolutepath" + filep.getAbsolutePath() + filep.getName());
           String rawschema=  readFile(schemaPath);

            // read in the Avro schema
           
           // Schema avroSchema = new Schema.Parser().parse(in);
            
            MessageType schema = MessageTypeParser.parseMessageType(rawschema);
            
            System.out.print(schema);
            
            job.getConfiguration().set("schema2", rawschema);
            // point to input data
            FileInputFormat.addInputPath(job, inputPath);
            job.setInputFormatClass(TextInputFormat.class);

            // set the output format
            job.setOutputFormatClass(ExampleOutputFormat.class);
            ExampleOutputFormat.setOutputPath(job, outputPath);
            ExampleOutputFormat.setSchema(job, schema);
            ExampleOutputFormat.setCompression(job, CompressionCodecName.GZIP);
            ExampleOutputFormat.setCompressOutput(job, true);

            // set a large block size to ensure a single row group.  see discussion
            ExampleOutputFormat.setBlockSize(job, 500 * 1024 * 1024);

            job.setMapperClass(ParquetMapper.class);
            job.setNumReduceTasks(0);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    
     private static String readFile(String path) throws IOException {
         BufferedReader reader = new BufferedReader(new FileReader(path));
         StringBuilder stringBuilder = new StringBuilder();
         try {
         String line = null;
         String ls = System.getProperty("line.separator");
         while ((line = reader.readLine()) != null ) {
         stringBuilder.append(line);
         stringBuilder.append(ls);
         System.out.print(path);
         System.out.print(stringBuilder);
         }
         } finally {
        // Utils.closeQuietly(reader);
         reader.close();
         }
         return stringBuilder.toString();
         }

     public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new TestCsvToParquet(), args);
            System.exit(exitCode);
        }
    }
    