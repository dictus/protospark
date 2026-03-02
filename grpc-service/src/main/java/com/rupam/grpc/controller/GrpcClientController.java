package com.rupam.grpc.controller;

import com.rupam.dto.Person;
import com.rupam.dto.PersonResponse;
import com.rupam.grpc.client.PersonServiceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller that wraps gRPC service
 * Allows testing gRPC via HTTP (suitable for Bruno, Postman, etc.)
 */
@RestController
@RequestMapping("/api/grpc/persons")
@Slf4j
public class GrpcClientController {

    @Autowired
    private PersonServiceClient grpcClient;

    /**
     * Create a person via gRPC
     * POST /api/grpc/persons/create?name=John&age=30
     */
    @PostMapping("/create")
    public ResponseEntity<PersonResponse> createPerson(
            @RequestParam String name,
            @RequestParam int age) {
        log.info("REST request: create person name={}, age={}", name, age);
        PersonResponse response = grpcClient.createPerson(name, age);
        return ResponseEntity.ok(response);
    }

    /**
     * Get a person via gRPC
     * GET /api/grpc/persons/get?name=John
     */
    @GetMapping("/get")
    public ResponseEntity<Person> getPerson(@RequestParam String name) {
        log.info("REST request: get person name={}", name);
        Person person = grpcClient.getPerson(name);
        return ResponseEntity.ok(person);
    }

    /**
     * Update a person via gRPC
     * PUT /api/grpc/persons/update?name=John&age=31
     */
    @PutMapping("/update")
    public ResponseEntity<PersonResponse> updatePerson(
            @RequestParam String name,
            @RequestParam int age) {
        log.info("REST request: update person name={}, age={}", name, age);
        PersonResponse response = grpcClient.updatePerson(name, age);
        return ResponseEntity.ok(response);
    }

    /**
     * List all persons via gRPC streaming
     * GET /api/grpc/persons/list
     */
    @GetMapping("/list")
    public ResponseEntity<String> listPersons() {
        log.info("REST request: list all persons");
        grpcClient.listPersons();
        return ResponseEntity.ok("Check logs for streamed persons");
    }

    /**
     * Health check
     * GET /api/grpc/persons/health
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("gRPC service is running on port 9090");
    }
}
