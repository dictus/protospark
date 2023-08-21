package com.rupam;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ProtoSparkApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProtoSparkApplication.class, args);
	}

	@Bean
	public JavaSparkContext sparkContext() {
		return new JavaSparkContext("local[*]", "ProtoSparkApplication");
	}

}
