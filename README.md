# beyond-text
Exploring efficient file formats which could be used for big data analytics

Understand the basic of different data/file format getting used for the big data and analytics purpose

Text
Sequence
Avro
Parquet
RCFile

1) Best use cases for each of them.

2) Their performance with wide and narrow query. (We dont really have to do this benchmark for all the format as quite good results are already available, we need this information so that other project can decide the right format using it)

3) Similarly storage wise comparison for all the format.

4) We need to explore all the tools which are helpful during development using any of these file format.

5) As data is mostly logged/collected in Text format, we need to prepare a hadoop job to convert Text to each of these format.

6) Hadoop job for using non text format as an input as well as output.

7) Compatibility of these file formats with different - different tools as impala,hive etc.

We have written basic hadoop map reduce jobs to perform conversion of text file in other available file formats.

Code is available at src/main/java/com for respective file formats. To execute hadoop jobs specific parameters need to be provided. 

Please find below steps to execute these hadoop jobs.

Below are the few initial steps which need to follow for running every hadoop job.

Section A

a). Pull beyond-text project from git hub to your local.
b). Modify build.gradle main class name with the required class name e.g. if we want to run job for AvroToParquetConversion make main class name as AvroToParquetConversion.
c). Run ./gradlew clean command to clean the old jar and then run ./gradlew build to create new jar.
d). Run ./gradlew build shadow command to create shadow jar.
e). Once shadow jar is created you can run hadoop job.

Steps to execute AvroToParquetConversion.java, which converts avro into parquet:-

a). Follow mentioned steps in section A.

b). Run below command to start hadoop job.
/usr/local/hadoop/bin/hadoop jar home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar hdfs:///avrotoparquet/twitter.avsc 
file:////home/file_format_testings/beyond-text/src/main/resources/twitter.avro 
file:////home/Documents/output_10

Here beyond-text-all.jar is shadow jar.
twitter.avsc is sample input avro file and is available in resource folder.
twitter.avro is schema file for twitter.avsc file and is available in resource folder.
output_10 is output directory.

Steps to run TextToAvro_2 job:-
 
a). Follow mentioned steps in section A(modify main class name with TextToAvro_2 in build.gradle file).

b). Run below command to start hadoop job.
/usr/local/hadoop/bin/hadoop jar home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar hdfs:///avrotoparquet/test.avsc 
file:////home/file_format_testings/beyond-text/src/main/resources/test.csv
file:////home/Documents/output_11

test.avsc is schema file for input file test.csv.
test.csv is input text file.
output_11 is output directory.

Steps to run AvroToText job:-
 
a). Follow mentioned steps in section A(modify main class name with AvroToText in build.gradle file).

b). Run below command to start hadoop job.
/usr/local/hadoop/bin/hadoop jar home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar hdfs:///avrotoparquet/test.avsc 
file:////home/file_format_testings/beyond-text/src/main/resources/twitter.avro
file:////home/Documents/output_11

test.avsc is schema file for input file twitter.avro.
twitter.avro is input text file.
output_11 is output directory.

Steps to run ParquetToCsv job:-

a). Follow mentioned steps in section A(modify main class name with ParquetToCsv in build.gradle file).

b). Run below command to start hadoop job.

/usr/local/hadoop/bin/hadoop jar /home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar gzip hdfs:///parquettocsv/part-m-00000.gz.parquet file:////home/output

gzip is compression type. Compression type can be snappy,lzo. 
part-m-00000.gz.parquet is input parquet file.
output is output directory.

Steps to run TestCsvToParquet job:-

a). Follow mentioned steps in section A(modify main class name with TestCsvToParquet in build.gradle file).

b). Run below command to start hadoop job.

/usr/local/hadoop/bin/hadoop jar /home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar hdfs:///csvtoparquet/test.schema hdfs:///csvtoparquet/test.csv file:////home/output

test.schema is schema file for input file test.csv.
test.csv is input text file.
output is output directory.

Steps to run ParquetToParquet job:-

a). Follow mentioned steps in section A(modify main class name with ParquetToParquet in build.gradle file).

b). Run below command to start hadoop job.

/usr/local/hadoop/bin/hadoop jar /home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar gzip hdfs:///parquettocsv/part-m-00000.gz.parquet file:////home/output_3

gzip is compression type. Compression type can be snappy,lzo. 
part-m-00000.gz.parquet is input parquet file.
output is output directory.

Steps to run SequenceToText job:-

a). Follow mentioned steps in section A(modify main class name with SequenceToText in build.gradle file).

b). Run below command to start hadoop job.

/usr/local/hadoop/bin/hadoop jar /home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar hdfs:///sequencetotext/test.sequence file:////home/output

test.sequence is input sequence file.
output is output directory.

Steps to run TextToSequence job:- 

a). Follow mentioned steps in section A(modify main class name with TextToSequence in build.gradle file).

b). Run below command to start hadoop job.

/usr/local/hadoop/bin/hadoop jar /home/file_format_testings/beyond-text/build/libs/beyond-text-all.jar hdfs:///sequencetotext/test.csv file:////home/output

test.csv is input csv file.
output is output directory.

