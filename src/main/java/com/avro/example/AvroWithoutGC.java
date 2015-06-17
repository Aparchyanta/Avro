package com.avro.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class AvroWithoutGC {
	public static void main(String[] args) throws IOException{
		Schema schema = new Schema.Parser().parse(new File("src/main/resources/AvroExample.avsc"));
		readTextWriteAvro(schema);
		readAvroWriteText(schema);
	}

	private static void readTextWriteAvro(Schema schema) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader("EmpReadFile.txt"));
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, new File("WCWriteAVRO.avro"));
		GenericRecord emp1 = new GenericData.Record(schema);
		
		String line = null;
		while ((line = br.readLine()) != null)   {
			String [] entries = line.split(",");
			
			emp1.put("id", Integer.parseInt(entries[0]));
			emp1.put("name", entries[1]);
			emp1.put("designation", entries[2]);
			emp1.put("mgrid", Integer.parseInt(entries[3]));
			emp1.put("hiredate", entries[4]);
			emp1.put("salary", Double.parseDouble(entries[5]));
			emp1.put("commission", Double.parseDouble(entries[6]));
			emp1.put("deptid", Integer.parseInt(entries[7]));
			dataFileWriter.append(emp1);
		}
		dataFileWriter.close();
	}

	private static void readAvroWriteText(Schema schema) throws IOException {
		// TODO Auto-generated method stub
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader=new DataFileReader<GenericRecord>(new File("WCWriteAVRO.avro"), datumReader);
		GenericRecord emp = null;
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("WCWriteText.txt"));
		
		while (dataFileReader.hasNext()) {
			emp = dataFileReader.next(emp);
			bw.write(emp.toString());
			bw.newLine();
		}
		bw.flush();
		bw.close();
		dataFileReader.close();
	}
	
	
}
