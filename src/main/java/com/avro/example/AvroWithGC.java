package com.avro.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;


public class AvroWithGC {
	public static void employeeDetails(Employee emp, String line){
		String [] entries = line.split(",");
		emp.setId(Integer.parseInt(entries[0]));
		emp.setName(entries[1]);
		emp.setDesignation(entries[2]);
		emp.setMgrid(Integer.parseInt(entries[3]));
		emp.setHiredate(entries[4]);
		emp.setSalary(Double.parseDouble(entries[5]));
		emp.setCommission(Double.parseDouble(entries[6]));
		emp.setDeptid(Integer.parseInt(entries[7]));
	}
	
	public static void readTextWriteAvro() throws IOException{
		
		String line=null;
		Employee obj = new Employee();
		BufferedReader br = new BufferedReader(new FileReader("EmpReadFile.txt"));
		DatumWriter<Employee> userDatumWriter = new SpecificDatumWriter<Employee>(Employee.class);
		DataFileWriter<Employee> dataFileWriter = new DataFileWriter<Employee>(userDatumWriter);
		dataFileWriter.create(obj.getSchema(), new File("ReadTextWriteAVRO.avro"));
		while ((line = br.readLine()) != null)   {
			employeeDetails(obj, line);
			dataFileWriter.append(obj);
          }
		dataFileWriter.close();
		br.close();
	}
	public static void readAvroWriteText() throws IOException{
		
		Employee emp=null;
		DatumReader<Employee> userDatumReader = new SpecificDatumReader<Employee>(Employee.class);
		DataFileReader<Employee> dataFileReader=new DataFileReader<Employee>(new File("ReadTextWriteAVRO.avro"), userDatumReader);
	    BufferedWriter bw = new BufferedWriter(new FileWriter("ReadTextWriteAVRO.txt"));
		while (dataFileReader.hasNext()) {
			emp = dataFileReader.next();
		
			bw.write(emp.toString());
			bw.newLine();
		}
		bw.flush();
		bw.close();
		dataFileReader.close();
		
	}
	public static void main(String[] args) throws IOException{
		readTextWriteAvro();
		readAvroWriteText();
	}
}
