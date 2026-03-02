package com.rupam.grpc.service;

import com.rupam.dto.Address;
import com.rupam.dto.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service layer for handling Person and Address protobuf operations
 */
@Service
@Slf4j
public class PersonService {

    /**
     * Create a Person protobuf message
     * @param name Person's name
     * @param age Person's age
     * @return Person protobuf message
     */
    public Person createPerson(String name, int age) {
        log.debug("Creating person with name: {} and age: {}", name, age);
        return Person.newBuilder()
                .setName(name)
                .setAge(age)
                .build();
    }

    /**
     * Create an Address protobuf message
     * @param streetName Address street name
     * @return Address protobuf message
     */
    public Address createAddress(String streetName) {
        log.debug("Creating address with street: {}", streetName);
        return Address.newBuilder()
                .setStreetName(streetName)
                .build();
    }

    /**
     * Validate person details
     * @param person Person to validate
     * @return true if valid, false otherwise
     */
    public boolean validatePerson(Person person) {
        if (person.getName() == null || person.getName().isEmpty()) {
            log.warn("Invalid person: name is empty");
            return false;
        }
        if (person.getAge() < 0 || person.getAge() > 150) {
            log.warn("Invalid person: age {} is out of range", person.getAge());
            return false;
        }
        log.info("Person validation passed for: {}", person.getName());
        return true;
    }

    /**
     * Validate address details
     * @param address Address to validate
     * @return true if valid, false otherwise
     */
    public boolean validateAddress(Address address) {
        if (address.getStreetName() == null || address.getStreetName().isEmpty()) {
            log.warn("Invalid address: street name is empty");
            return false;
        }
        log.info("Address validation passed for: {}", address.getStreetName());
        return true;
    }

    /**
     * Get person information as formatted string
     * @param person Person protobuf message
     * @return Formatted person information
     */
    public String getPersonInfo(Person person) {
        return String.format("Person{name='%s', age=%d}", person.getName(), person.getAge());
    }

    /**
     * Get address information as formatted string
     * @param address Address protobuf message
     * @return Formatted address information
     */
    public String getAddressInfo(Address address) {
        return String.format("Address{street='%s'}", address.getStreetName());
    }
}
