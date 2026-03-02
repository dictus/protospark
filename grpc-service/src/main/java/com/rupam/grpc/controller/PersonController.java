package com.rupam.grpc.controller;

import com.rupam.dto.Address;
import com.rupam.dto.Person;
import com.rupam.grpc.service.PersonService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller that demonstrates usage of protodata module
 * Uses protobuf-generated Person and Address classes
 */
@RestController
@RequestMapping("/api/persons")
@Slf4j
public class PersonController {

    @Autowired
    private PersonService personService;

    /**
     * Create a person with the given name and age
     * @param name Person's name
     * @param age Person's age
     * @return Person protobuf message as JSON
     */
    @PostMapping("/create")
    public ResponseEntity<PersonResponse> createPerson(
            @RequestParam String name,
            @RequestParam int age) {
        
        Person person = personService.createPerson(name, age);
        
        if (!personService.validatePerson(person)) {
            return ResponseEntity.badRequest().build();
        }
        
        log.info("Created person: {}", personService.getPersonInfo(person));
        
        PersonResponse response = new PersonResponse();
        response.setName(person.getName());
        response.setAge(person.getAge());
        response.setMessage("Person created successfully");
        
        return ResponseEntity.ok(response);
    }

    /**
     * Get a sample person
     * @return Sample Person object with address
     */
    @GetMapping("/sample")
    public ResponseEntity<PersonResponse> getSamplePerson() {
        Person person = personService.createPerson("John Doe", 30);
        Address address = personService.createAddress("123 Main Street");
        
        log.info("Retrieved sample person: {}, Address: {}", 
                personService.getPersonInfo(person), 
                personService.getAddressInfo(address));
        
        PersonResponse response = new PersonResponse();
        response.setName(person.getName());
        response.setAge(person.getAge());
        response.setStreetName(address.getStreetName());
        response.setMessage("Sample person retrieved");
        
        return ResponseEntity.ok(response);
    }

    /**
     * Store person and address
     * @param name Person's name
     * @param age Person's age
     * @param street Street address
     * @return Response with person and address details
     */
    @PostMapping("/store")
    public ResponseEntity<PersonResponse> storePerson(
            @RequestParam String name,
            @RequestParam int age,
            @RequestParam String street) {
        
        Person person = personService.createPerson(name, age);
        Address address = personService.createAddress(street);
        
        // Validate both objects
        if (!personService.validatePerson(person) || !personService.validateAddress(address)) {
            return ResponseEntity.badRequest().build();
        }
        
        log.info("Stored person: {}, Address: {}", 
                personService.getPersonInfo(person), 
                personService.getAddressInfo(address));
        
        PersonResponse response = new PersonResponse();
        response.setName(person.getName());
        response.setAge(person.getAge());
        response.setStreetName(address.getStreetName());
        response.setMessage("Person and address stored successfully");
        
        return ResponseEntity.ok(response);
    }

    /**
     * Inner class to wrap Person and Address in JSON response
     */
    public static class PersonResponse {
        private String name;
        private int age;
        private String streetName;
        private String message;

        public PersonResponse() {}

        public PersonResponse(String name, int age, String streetName) {
            this.name = name;
            this.age = age;
            this.streetName = streetName;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getStreetName() {
            return streetName;
        }

        public void setStreetName(String streetName) {
            this.streetName = streetName;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
