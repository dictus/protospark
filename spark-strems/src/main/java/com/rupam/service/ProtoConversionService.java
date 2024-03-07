package com.rupam.service;

import com.google.protobuf.GeneratedMessageV3;
import com.rupam.dto.Person;
import com.rupam.model.ProtoToBeanConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProtoConversionService {

    @Autowired
    private ProtoToBeanConverter converter;

    public Object convertProtoToBean(GeneratedMessageV3 protoObject) {
        return converter.convert(protoObject, Person.class);
    }
}