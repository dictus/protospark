package com.rupam.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.rupam.dto.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
public class SparkJob {

    @Autowired
    @Qualifier("mySparkC")
    private JavaSparkContext sparkContext;

    public long countWordsInSentences(@NotNull List<String> sentences) {

        JavaRDD<String> sentencesRDD = sparkContext.parallelize(sentences);
        JavaRDD<String> words = sentencesRDD.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

        return words.count();
    }

    @Autowired
    SparkSession sparkSession;

    public String readJson (@NotNull List<String> sentences) throws  InvalidProtocolBufferException {
       /* Person person = Person.newBuilder().setName("Arundhati").setAge(3).build();
        String json = JsonFormat.printer().print(person);
        log.info("TEst {} ",json);*/
        Dataset<Row> rowDataset = sparkSession.read().json("spark-strems/src/main/resources/ReadJson.json");
        return rowDataset.schema().json();
    }

}