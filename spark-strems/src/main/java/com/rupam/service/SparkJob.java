package com.rupam.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.rupam.dto.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SparkJob {

    @Autowired
    @Qualifier("mySparkC")
    private JavaSparkContext sparkContext;

    public long countWordsInSentences(@NotNull List<String> sentences) {
        // Suggestion 4: Filter out null or empty sentences
        List<String> filteredSentences = sentences.stream()
                .filter(sentence -> sentence != null && !sentence.isEmpty())
                .collect(Collectors.toList());

        // Suggestion 2: Let Spark decide the optimal number of partitions or use a configurable value
        JavaRDD<String> sentencesRDD = sparkContext.parallelize(filteredSentences);

        // Suggestion 1: Compile the pattern once and reuse it
        Pattern pattern = Pattern.compile(" ");

        // Suggestion 3: Use flatMapToPair to create a pair RDD with words as keys and 1 as values
        JavaPairRDD<String, Integer> wordPairs = sentencesRDD.flatMapToPair(sentence -> Arrays.stream(pattern.split(sentence)).map(word -> new Tuple2<>(word, 1)).iterator());

        return wordPairs.distinct().count();
    }

    @Autowired
    SparkSession sparkSession;

    public String readJson (@NotNull List<String> sentences) throws  InvalidProtocolBufferException {
       /* Person person = Person.newBuilder().setName("Arundhati").setAge(3).build();
        String json = JsonFormat.printer().print(person);
        log.info("TEst {} ",json);*/
        Dataset<Row> rowDataset = sparkSession.read().json("src/main/resources/ReadJson.json");
        return rowDataset.schema().json();
    }


    public String readCsv (@NotNull String path) throws  InvalidProtocolBufferException {
       /* Person person = Person.newBuilder().setName("Arundhati").setAge(3).build();
        String json = JsonFormat.printer().print(person);
        log.info("TEst {} ",json);*/
        /*Dataset<Row> rowDataset = sparkSession.read().format("csv").schema(minimumCustomerDataSchema()).
                options(Map.of("header","true","inferSchema","false")).load("spark-strems/src/main/resources/ReadCSV.csv")
                ;*/
       /* Dataset<Person> personDataset = sparkSession.read().format("csv").schema(minimumCustomerDataSchema()).
                options(Map.of("header","true","inferSchema","false")).
        csv("spark-strems/src/main/resources/ReadCSV.csv")
                .as(Encoders.javaSerialization(Person.class));*/

        Dataset<Row> csvDataset = sparkSession.read()
                .format("csv")
                .options(
                        new HashMap<String, String>() {{
                            put("header", "true");
                            put("inferSchema", "false");
                        }}
                )
                .csv("src/main/resources/ReadCSV.csv");

        GenericMapFunction genericMapFunction = new GenericMapFunction();
        // Convert the CSV Dataset to a Dataset<Person> using custom conversion
        Dataset<Person> personDataset = csvDataset.map(
                (MapFunction<Row, Person>) row -> genericMapFunction.call(row),
                Encoders.javaSerialization(Person.class)
        );


        /*Dataset<Row> output = personDataset
                .select(from_protobuf(col("value"), "com.rupam.dto.Person").as("event"));

        output.write().json("out/proJso");*/

        Dataset<String> stringDataset = personDataset.map((MapFunction<Person, String>) row -> {
            String json = null;
            try {
                json = JsonFormat.printer().print(row);
            } catch (Exception e) {
                log.error("Eroro {} ", e);

            }
            return json;
        }, Encoders.STRING());
        stringDataset.write().mode(SaveMode.Overwrite).json("out/jsonscc/");

        log.atInfo().log("Data Set {}", personDataset.count());
        personDataset.write().mode(SaveMode.Overwrite).json("output/json");
        return personDataset.schema().json();
    }




    /*public static StructType minimumCustomerDataSchema() {
        return DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)}
        );
    }*/
}