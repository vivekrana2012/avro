package com.demo.avro.specific;

import com.demo.avro.Person;

public class SpecificRecordExample {

    public static void main(String[] args) {

        Person.Builder personBuilder = Person.newBuilder();
        personBuilder.setId(1);
        personBuilder.setEmailAddress("vivekrana.2012@gmail.com");
        personBuilder.setPhoneNumber("9643538188");
        personBuilder.setUsername("vivekrana.2012");
        personBuilder.setFirstName("vivek");
        personBuilder.setLastName("rana");

        Person person = personBuilder.build();
        System.out.println(person.toString());
    }
}
