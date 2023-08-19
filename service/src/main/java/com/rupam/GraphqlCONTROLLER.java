package com.rupam;

import foo.bar.Person;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GraphqlCONTROLLER {

    @GetMapping("test")
    public ResponseEntity<Object> getProto(){
        return ResponseEntity.of(Person.newBuilder().setName("Arundhati").setAge(3).build());
    }
}
