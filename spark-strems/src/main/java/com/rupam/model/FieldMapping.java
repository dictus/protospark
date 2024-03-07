package com.rupam.model;

import lombok.Data;

import java.io.Serializable;

@Data

public class FieldMapping  implements Serializable {
    private String columnName;
    private String fieldType;
    private int columnIndex;
    private String colmSPel;

    // Getters and setters
    // Define appropriate getters and setters for the fields
}
