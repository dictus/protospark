package com.rupam.service;

import com.rupam.config.CsvConfigProperties;
import com.rupam.dto.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.jvnet.hk2.annotations.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.Serializable;


public class GenericMapFunction2Serv implements MapFunction<Row, Person>  {

    public CsvConfigProperties getCsvConfigProperties() {
        return csvConfigProperties;
    }

    public void setCsvConfigProperties(CsvConfigProperties csvConfigProperties) {
        this.csvConfigProperties = csvConfigProperties;
    }

    CsvConfigProperties csvConfigProperties;

    private static Person parsePersonFromRow(Row row,CsvConfigProperties csvConfigProperties) {
        // Implement your logic here to create a Person object from the Row
        // For example:
        System.out.println("row {}" + row.length());

        /*String name = row.getString(0);
        System.out.println("name {}" + name);// Assuming "name" is in the first column
        Integer age = Integer.parseInt(row.get(1).toString()); // Assuming "age" is in the second column
        System.out.printf(" data {%s}", age);
        Person person = Person.newBuilder()
                .setName(name)
                .setAge(age)
                .build();*/

        Person person = Person.newBuilder().
                setName(row.get(csvConfigProperties.getMapping().get(0).getColumnIndex()).toString())
                .setAge(Integer.parseInt(
                        row.get(row.fieldIndex(csvConfigProperties.getMapping().get(1).getColumnName())).toString()))
                .build();



        return person;
    }



    @Override
    public Person call(Row t) throws Exception {


        return parsePersonFromRow(t,csvConfigProperties);
    }

}
