package com.rupam.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean("mySparkC")
    public JavaSparkContext sparkContext() {

        SparkSession sparkSession = getSession();

        return new JavaSparkContext(sparkSession.sparkContext());
    }

    @Bean("sparkSession")
    public SparkSession getSession() {
        SparkSession.Builder builder = SparkSession.builder();

        builder.appName("ProtoSparkApplication");
        builder.master("local[*]");
        builder.config("spark.ui.enabled",true);
        SparkSession sparkSession = builder.getOrCreate();
        return sparkSession;
    }

}
