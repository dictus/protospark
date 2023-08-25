package com.rupam;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationExtensionsKt;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ProtoSparkApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProtoSparkApplication.class, args);
	}

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
		builder.config("spark.ui.enabled",false);
		SparkSession sparkSession = builder.getOrCreate();
		return sparkSession;
	}

}
