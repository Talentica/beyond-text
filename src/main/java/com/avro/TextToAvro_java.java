package com.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class TextToAvro_java {

	public static void main(String args[]) throws IOException {
		Schema s = new Schema.Parser().parse(new File(
				"/home/nikita/Downloads/twitter_bkp.avsc"));
		//GenericRecord gr = new GenericData.Record(s);
		String splitBy = ",";
		BufferedReader br = new BufferedReader(new FileReader(
				"/home/nikita/Downloads/output_7/part-m-00000"));
		String line;
		String[] b = null;
		while ((line = br.readLine()) != null) {
			b = line.split(splitBy);

		}
		GenericRecord e1 = new GenericData.Record(s);
		for (int i = 0; i < b.length; i++) {
			System.out.println(b[i]);
			e1.put(i, b[i]);
		}
		br.close();

		System.out.print(e1);

		// converts Java objects into in-memory serialized format
		// System.out.print(s);;

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				s);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(s, new File("/home/nikita/Desktop/test3.avro"));
		dataFileWriter.append(e1);
		dataFileWriter.close();

	}

}
