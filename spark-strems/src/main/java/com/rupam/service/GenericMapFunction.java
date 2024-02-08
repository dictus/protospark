package com.rupam.service;

import com.rupam.dto.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class GenericMapFunction implements MapFunction<Row, Person> {

    private static Person parsePersonFromRow(Row row) {
        // Implement your logic here to create a Person object from the Row
        // For example:
        System.out.println("row {}" + row.length());

        String name = row.getString(0);
        System.out.println("name {}" + name);// Assuming "name" is in the first column
        Integer age = Integer.parseInt(row.get(1).toString()); // Assuming "age" is in the second column
        System.out.printf(" data {%s}", age);
        Person person = Person.newBuilder()
                .setName(name)
                .setAge(age)
                .build();

        return person;
    }

    @Override
    public Person call(Row t) throws Exception {


        return parsePersonFromRow(t);
    }

}
