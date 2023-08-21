package com.rupam.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class SparkJob {

    public long countWordsInSentences(JavaSparkContext sparkContext, JavaRDD<String> sentences) {
        JavaRDD<String> words = sentences.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        return words.count();
    }
}