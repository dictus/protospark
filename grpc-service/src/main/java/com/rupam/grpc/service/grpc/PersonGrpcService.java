package com.rupam.grpc.service.grpc;

import com.rupam.dto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * gRPC Service Implementation for Person operations
 * Uses Protocol Buffers for serialization
 * Implements net.devh grpc-spring-boot-starter approach
 */
@GrpcService
@Slf4j
public class PersonGrpcService extends PersonServiceGrpc.PersonServiceImplBase {

    @Autowired
    private com.rupam.grpc.service.PersonService personService;

    // In-memory storage for demonstration
    private static final List<Person> personDatabase = new CopyOnWriteArrayList<>();

    /**
     * Create a new person via gRPC
     */
    @Override
    public void createPerson(PersonRequest request, StreamObserver<PersonResponse> responseObserver) {
        log.info("gRPC CreatePerson called for: {}", request.getName());

        try {
            // Validate input
            if (request.getName() == null || request.getName().isEmpty()) {
                responseObserver.onError(new IllegalArgumentException("Name cannot be empty"));
                return;
            }

            // Create person using service
            Person person = personService.createPerson(request.getName(), request.getAge());

            // Validate
            if (!personService.validatePerson(person)) {
                responseObserver.onError(new IllegalArgumentException("Invalid person data"));
                return;
            }

            // Store in database
            personDatabase.add(person);

            // Build response
            PersonResponse response = PersonResponse.newBuilder()
                    .setName(person.getName())
                    .setAge(person.getAge())
                    .setMessage("Person created successfully via gRPC")
                    .build();

            log.info("Person created: {}", personService.getPersonInfo(person));
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error creating person", e);
            responseObserver.onError(e);
        }
    }

    /**
     * Get a person by name via gRPC
     */
    @Override
    public void getPerson(PersonRequest request, StreamObserver<Person> responseObserver) {
        log.info("gRPC GetPerson called for: {}", request.getName());

        try {
            Person person = personDatabase.stream()
                    .filter(p -> p.getName().equals(request.getName()))
                    .findFirst()
                    .orElse(null);

            if (person != null) {
                log.info("Found person: {}", personService.getPersonInfo(person));
                responseObserver.onNext(person);
            } else {
                log.warn("Person not found: {}", request.getName());
                responseObserver.onError(new RuntimeException("Person not found"));
                return;
            }

            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error getting person", e);
            responseObserver.onError(e);
        }
    }

    /**
     * Update an existing person via gRPC
     */
    @Override
    public void updatePerson(Person request, StreamObserver<PersonResponse> responseObserver) {
        log.info("gRPC UpdatePerson called for: {}", request.getName());

        try {
            // Find and remove old person
            boolean removed = personDatabase.removeIf(p -> p.getName().equals(request.getName()));

            if (!removed) {
                responseObserver.onError(new RuntimeException("Person not found for update"));
                return;
            }

            // Add updated person
            personDatabase.add(request);

            PersonResponse response = PersonResponse.newBuilder()
                    .setName(request.getName())
                    .setAge(request.getAge())
                    .setMessage("Person updated successfully via gRPC")
                    .build();

            log.info("Person updated: {}", personService.getPersonInfo(request));
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error updating person", e);
            responseObserver.onError(e);
        }
    }

    /**
     * List all persons as a streaming response via gRPC
     * Server-side streaming: server sends multiple responses
     */
    @Override
    public void listPersons(PersonRequest request, StreamObserver<Person> responseObserver) {
        log.info("gRPC ListPersons called, returning {} persons", personDatabase.size());

        try {
            // Stream all persons to client
            for (Person person : personDatabase) {
                log.debug("Streaming person: {}", personService.getPersonInfo(person));
                responseObserver.onNext(person);
            }

            log.info("Streaming completed for {} persons", personDatabase.size());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error listing persons", e);
            responseObserver.onError(e);
        }
    }

    /**
     * Get statistics about persons in database
     */
    public int getPersonCount() {
        return personDatabase.size();
    }

    /**
     * Clear all persons
     */
    public void clearPersons() {
        personDatabase.clear();
        log.info("Person database cleared");
    }
}
