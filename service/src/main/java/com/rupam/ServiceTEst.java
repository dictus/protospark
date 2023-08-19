package com.rupam;

import foo.bar.Person;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ServiceTEst implements ApplicationRunner {
    @Override
    public void run(ApplicationArguments args) throws Exception {

        Person person = Person.newBuilder().setName("Arundhati").setAge(3).build();

        System.out.println(person);



    }
}
