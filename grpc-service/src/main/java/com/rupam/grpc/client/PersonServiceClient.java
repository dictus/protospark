package com.rupam.grpc.client;

import com.rupam.dto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Client for testing PersonService
 */
@Component
@Slf4j
public class PersonServiceClient {

    private final ManagedChannel channel;
    private final PersonServiceGrpc.PersonServiceBlockingStub blockingStub;

    public PersonServiceClient() {
        // Connect to gRPC server on port 9090
        this.channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                .usePlaintext()
                .build();
        this.blockingStub = PersonServiceGrpc.newBlockingStub(channel);
        log.info("gRPC Client initialized, connected to localhost:9090");
    }

    /**
     * Create a person
     */
    public PersonResponse createPerson(String name, int age) {
        log.info("Creating person via gRPC: name={}, age={}", name, age);
        PersonRequest request = PersonRequest.newBuilder()
                .setName(name)
                .setAge(age)
                .build();
        
        PersonResponse response = blockingStub.createPerson(request);
        log.info("Person created: {}", response.getMessage());
        return response;
    }

    /**
     * Get a person
     */
    public Person getPerson(String name) {
        log.info("Getting person via gRPC: name={}", name);
        PersonRequest request = PersonRequest.newBuilder()
                .setName(name)
                .build();
        
        Person person = blockingStub.getPerson(request);
        log.info("Person retrieved: name={}, age={}", person.getName(), person.getAge());
        return person;
    }

    /**
     * Update a person
     */
    public PersonResponse updatePerson(String name, int age) {
        log.info("Updating person via gRPC: name={}, age={}", name, age);
        Person person = Person.newBuilder()
                .setName(name)
                .setAge(age)
                .build();
        
        PersonResponse response = blockingStub.updatePerson(person);
        log.info("Person updated: {}", response.getMessage());
        return response;
    }

    /**
     * List all persons (server streaming)
     */
    public void listPersons() {
        log.info("Listing all persons via gRPC streaming...");
        PersonRequest request = PersonRequest.newBuilder()
                .setName("")
                .build();
        
        Iterator<Person> persons = blockingStub.listPersons(request);
        int count = 0;
        while (persons.hasNext()) {
            Person person = persons.next();
            log.info("Person {}: name={}, age={}", ++count, person.getName(), person.getAge());
        }
        log.info("Total persons streamed: {}", count);
    }

    /**
     * Shutdown the client
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        log.info("gRPC Client shutdown");
    }
}
