package com.rupam.config;

import com.google.protobuf.GeneratedMessageV3;
import com.rupam.service.ProtoConversionService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;

@Configuration
public class ProtoConversionConfig {

    @Bean
    public Converter<GeneratedMessageV3, Object> protobufToBeanConverter(ProtoConversionService protoConversionService) {
        return new Converter<GeneratedMessageV3, Object>() {
            @Override
            public Object convert(GeneratedMessageV3 source) {
                return protoConversionService.convertProtoToBean(source);
            }
        };
    }
}
