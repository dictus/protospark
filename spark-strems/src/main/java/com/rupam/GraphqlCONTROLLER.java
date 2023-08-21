package com.rupam;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.rupam.dto.Person;
import com.rupam.service.SparkJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;

@RestController
public class GraphqlCONTROLLER {

   /* @GetMapping("test")
    public Flux<String> getProto() throws InvalidProtocolBufferException {
        //JsonFormat.parser().ignoringUnknownFields().merge(json, structBuilder);
        Person arundhati = Person.newBuilder().setName("Arundhati").setAge(3).build();
        return Flux.fromIterable(Arrays.asList(JsonFormat.printer().print(arundhati)));
        //of();
    }*/

  /*  @Autowired
    WordCountService service;*/

    /*@PostMapping("readFile")
    public Flux<Object> getProtoSpark(List<String> string) throws InvalidProtocolBufferException {
        return Flux.fromIterable(service.getCount(string).values());
    }*/


    @Autowired
    private JavaSparkContext sparkContext;

    @Autowired
    private SparkJob sparkJob;
    @GetMapping("/count-words")
    public Mono<Long> countWords() {
        JavaRDD<String> sentences = sparkContext.parallelize(Arrays.asList(
                "Hello world",
                "Spring WebFlux and Spark integration",
                "Reactive programming"
        ));

        long wordCount = sparkJob.countWordsInSentences(sparkContext, sentences);
        return Mono.just(wordCount);
    }
}
