package com.rupam.service;

import com.google.protobuf.util.JsonFormat;
import com.rupam.config.CsvConfigProperties;
import com.rupam.dto.Person;
import com.rupam.model.FieldMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;

@Component
@Slf4j
public class CsvProcessor {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private CsvConfigProperties csvConfigProperties;

    @Autowired
    private ProtoConversionService protoConversionService;



    public void processCsv() {
        //try {
            // Define schema based on CSV configuration
            StructType schema = getSchemaFromCsvConfig();

            // Read CSV file with specified schema
            Dataset<Row> csvDataFrame = sparkSession.read()
                    .format("csv")
                    .schema(schema)
                    .options(Map.of("header", String.valueOf(csvConfigProperties.isHeader()),
                            "inferSchema", String.valueOf(csvConfigProperties.isInferSchema())))
                    .csv(csvConfigProperties.getPath());
            csvDataFrame.schema();

        GenericMapFunction2Serv serv = new GenericMapFunction2Serv();
        serv.setCsvConfigProperties(csvConfigProperties);
        // Convert the CSV Dataset to a Dataset<Person> using custom conversion
        Dataset<Person> personDataset = csvDataFrame.map(
                (MapFunction<Row, Person>) row -> serv.call(row),
                Encoders.javaSerialization(Person.class)
        );

        personDataset.schema();

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





        // Convert DataFrame to Protobuf Dataset
            /*Dataset<Person> protoDataset = csvDataFrame.as(Encoders.javaSerialization(Person.class));
             protoDataset.schema();

            System.out.println("Datatatatatat===================");
             protoDataset.show(2);
            // Use converted Protobuf dataset
            protoDataset.foreach(protoObject -> {
                // Convert Protobuf object to Java bean
                Object bean = protoConversionService.convertProtoToBean(protoObject);

                // Use the converted Java bean (e.g., persist to database, process further, etc.)
                System.out.println("Converted Java bean: " + bean);
            });*/
      /*  } catch (Exception e) {
            // Handle any exceptions
            e.printStackTrace();
        }*/
    }

    private StructType getSchemaFromCsvConfig() {
        StructField[] fields = csvConfigProperties.getMapping().stream()
                .sorted(Comparator.comparingInt(FieldMapping::getColumnIndex))
                .map(field -> DataTypes.createStructField(field.getColumnName(), getSparkDataType(field.getFieldType()), true))
                .toArray(StructField[]::new);
        return DataTypes.createStructType(fields);
        }


    private org.apache.spark.sql.types.DataType getSparkDataType(String fieldType) {
        switch (fieldType.toUpperCase()) {
            case "STRING":
                return DataTypes.StringType;
            case "INT":
                return DataTypes.IntegerType;
            // Add support for more data types as needed
            default:
                throw new IllegalArgumentException("Unsupported data type: " + fieldType);
        }
    }
}
