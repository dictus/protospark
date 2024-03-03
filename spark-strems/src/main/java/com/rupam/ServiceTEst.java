package com.rupam;

import com.rupam.dto.Person;
import com.rupam.service.CsvProcessor;
import com.rupam.service.SparkJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class ServiceTEst implements ApplicationRunner {

    @Autowired
    SparkJob sparkJob;

    @Autowired
    CsvProcessor csvProcessor;

    @Override
    public void run(ApplicationArguments args) throws Exception {

      //  Person person = Person.newBuilder().setName("Arundhati").setAge(3).build();

      //  sparkJob.readJson(Arrays.asList("as"));
       // sparkJob.readCsv("abcpath");

        //System.out.println(person);

        csvProcessor.processCsv();
    }
}
