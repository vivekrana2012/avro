package com.demo.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExample {

    public static void main(String[] args) {
        // define schema
        Schema.Parser parser = new Schema.Parser();

        Schema schema = parser.parse("{\n" +
                "  \"namespace\": \"com.demo.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Person\",\n" +
                "  \"doc\": \"Demo Class for avro\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"id\", \"type\": \"int\"},\n" +
                "    {\"name\": \"username\",  \"type\": [\"string\", \"null\"]},\n" +
                "    {\"name\": \"email_address\",  \"type\": [\"string\", \"null\"]},\n" +
                "    {\"name\": \"phone_number\",  \"type\": [\"string\", \"null\"]},\n" +
                "    {\"name\": \"first_name\",  \"type\": [\"string\", \"null\"]},\n" +
                "    {\"name\": \"last_name\",  \"type\": [\"string\", \"null\"]}\n" +
                "  ]\n" +
                "}");

        // create object
        GenericRecordBuilder personBuilder = new GenericRecordBuilder(schema);
        personBuilder.set("id", 1);
        personBuilder.set("username", "vivekrana.2012");
        personBuilder.set("email_address", "vivekrana.2012@gmail.com");
        personBuilder.set("phone_number", "9643538188");
        personBuilder.set("first_name", "vivek");
        personBuilder.set("last_name", "rana");

        GenericData.Record person = personBuilder.build();

        System.out.println(person);

        // write to file
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.create(schema, new File("person_generic.avro"));
            dataFileWriter.append(person);
            System.out.println("Success in writing to avro file.");
        }catch (IOException ex){
            ex.printStackTrace();
        }
        // read from file
        File file = new File("person_generic.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        GenericRecord genericRecord;
        try(DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader)){
            genericRecord = dataFileReader.next();
            System.out.println("Read Success!");
            System.out.println(genericRecord.toString());
            System.out.println("username - " + genericRecord.get("username"));
        } catch (IOException ex){
            ex.printStackTrace();
        }
    }
}
