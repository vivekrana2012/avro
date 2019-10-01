package com.demo.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class ReflectionRecordExample {

    public static void main(String[] args) {

        Schema schema = ReflectData.get().getSchema(Person.class);

        System.out.println(schema.toString(true));
    }
}
