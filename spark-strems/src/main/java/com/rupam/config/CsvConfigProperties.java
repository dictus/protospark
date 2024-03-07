package com.rupam.config;

import com.rupam.model.FieldMapping;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "csv")
@Data
public class CsvConfigProperties implements Serializable {
    private String path;
    private boolean header;
    private boolean inferSchema;
    private List<FieldMapping> mapping;
    private String protobufClass;
    // Getters and setters
    // Define appropriate getters and setters for the fields
}
