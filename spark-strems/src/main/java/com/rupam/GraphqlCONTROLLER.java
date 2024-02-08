package com.rupam;

import com.rupam.service.SparkJob;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

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
    private SparkJob sparkJob;
    @GetMapping("/count-words")
    public ResponseEntity<Long> countWords() {
        List<String> sentences = Arrays.asList(
                "Hello world",
                "Spring WebFlux and Spark integration",
                "Reactive programming"
        );
        return new ResponseEntity<>( sparkJob.countWordsInSentences(sentences), HttpStatus.ACCEPTED);
    }
}
