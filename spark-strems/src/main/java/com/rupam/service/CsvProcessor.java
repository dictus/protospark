package com.rupam.service;

import com.google.protobuf.util.JsonFormat;
import com.rupam.config.CsvConfigProperties;
import com.rupam.dto.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class CsvProcessor {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private CsvConfigProperties csvConfigProperties;

    private GenericMapFunction2Serv<Person> mapFunction;

    public void processCsv() {
        Dataset<Row> csvDataFrame = sparkSession.read()
                .format("csv")
                .options(Map.of(
                        "header", String.valueOf(csvConfigProperties.isHeader()),
                        "inferSchema", String.valueOf(csvConfigProperties.isInferSchema())))
                .csv(csvConfigProperties.getPath());
        mapFunction = new GenericMapFunction2Serv<>(Person.class,csvConfigProperties);
        // Apply map function to convert each row to Person object
        Dataset<Person> personDataset = csvDataFrame.map(mapFunction, Encoders.javaSerialization(Person.class));

        // Show the resulting dataset

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
    }
}
