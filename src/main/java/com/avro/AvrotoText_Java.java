package com.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvrotoText_Java {

	public static void main(String args[]) throws IOException {
		/* code for parsing with customized schema file*/

		Schema s = new Schema.Parser().parse(new File(
				"/home/nikita/Downloads/twitter_bkp.avsc"));
		System.out.println(s);

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				s);
		org.apache.avro.file.FileReader<GenericRecord> fileReader2 = DataFileReader
				.openReader(new File("/home/nikita/Desktop/test3.avro"),
						datumReader);

		for (GenericRecord datum : fileReader2) {
			System.out.println("value = " + datum);
			System.out.println("schema = " + datum.getSchema());
		}

		/* code for parsing with in-built schema of file */

		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		org.apache.avro.file.FileReader<GenericRecord> fileReader = DataFileReader
				.openReader(new File("/home/nikita/Desktop/test3.avro"), reader);

		for (GenericRecord datum : fileReader) {
			System.out.println("value = " + datum);
			System.out.println("schema = " + datum.getSchema());
		}

		fileReader.close();

	}

}
