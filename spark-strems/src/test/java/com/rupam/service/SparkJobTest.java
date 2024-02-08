package com.rupam.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


@SpringBootTest
class SparkJobTest {

    @Autowired
    SparkJob sparkJob;


    // Tests that the method correctly counts the number of distinct words in a list of sentences containing multiple words
    @Test
    public void test_countWordsInSentences_multipleWords() {
        // Arrange
        List<String> sentences = Arrays.asList("Hello world", "This is a test", "Hello world");

        // Act
        long result = sparkJob.countWordsInSentences(sentences);

        // Assert // non distinct
        assertNotEquals(5, result);

        assertEquals(6, result);

    }

    // Tests that the method correctly counts the number of distinct words in a list of sentences containing a single word
    @Test
    public void test_countWordsInSentences_singleWord() {
        // Arrange
        List<String> sentences = Arrays.asList("Hello", "Hello", "Hello");

        // Act
        long result = sparkJob.countWordsInSentences(sentences);

        // Assert
        assertEquals(1, result);
    }

    // Tests that the method returns 0 when given an empty list of sentences
    @Test
    public void test_countWordsInSentences_emptyList() {
        // Arrange
        List<String> sentences = new ArrayList<>();
        long expectedCount = 0;

        // Act
        long actualCount = sparkJob.countWordsInSentences(sentences);

        // Assert
        assertEquals(expectedCount, actualCount);
    }

}