package com.rupam.service;

import com.rupam.config.CsvConfigProperties;
import com.rupam.dto.Person;
import com.rupam.model.FieldMapping;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import static java.lang.reflect.AccessibleObject.setAccessible;

@Component
@Slf4j
public class GenericMapFunction2Serv<T> implements MapFunction<Row, T>, Serializable {


    private final CsvConfigProperties csvConfigProperties;

    private final Class<T> targetType;

    public GenericMapFunction2Serv(Class<T> targetType, CsvConfigProperties csvConfigProperties) {
        this.targetType = targetType;
        this.csvConfigProperties = csvConfigProperties;

    }


    private List<FieldMapping> columnMappings;


    private T parseObjectFromRow(Row row) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchFieldException {
        // Dynamically map columns from the row based on mappings obtained from YAML
        SpelExpressionParser parser = new SpelExpressionParser();
        StandardEvaluationContext context = new StandardEvaluationContext();
        context.setVariable("row", row);
        Class<?> messageClass = Class.forName(csvConfigProperties.getProtobufClass());
        Object builder = messageClass.getMethod("newBuilder").invoke(null);
        // Object build =
        // Set field values dynamically using reflection
       /* messageClass.getMethod(csvConfigProperties.getMapping().get(0).getColumnName(), String.class).invoke(builder, "value1");
        messageClass.getMethod("setField2", int.class).invoke(builder, 123);*/

        // Build the message
        /*MyMessage message = (MyMessage) messageClass.getMethod("build").invoke(builder);

        T object = targetType.get*/
        columnMappings = csvConfigProperties.getMapping();
        /*for (FieldMapping fieldMapping : columnMappings) {
            String columnName = fieldMapping.getColumnName();
            String expression = String.valueOf(fieldMapping.getColmSPel());

            // Parse SpEL expression to get column value
            Expression exp = parser.parseExpression(expression);
            Object columnValue = exp.getValue(row, Object.class);
            log.info("What {}",columnValue);
            // Set column value to the object dynamically using reflection
            try {
                targetType.getDeclaredField(columnName).setAccessible(true);
                targetType.getDeclaredField(columnName).set(builder, columnValue);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace(); // Handle or log the exception accordingly
            }
        }*/
        for (FieldMapping fieldMapping : columnMappings) {
            String columnName = fieldMapping.getColumnName();
            String expression = fieldMapping.getColmSPel();

            // Parse SpEL expression to get column value
            log.debug("SpEL expression: {}", expression);
            log.debug("Row object: {}", row);
            Expression exp = parser.parseExpression(expression);
            Object columnValue = exp.getValue(row, Object.class);

            // Set column value to the object dynamically using reflection


            Field declaredField = builder.getClass().getDeclaredField(columnName + "_");
            declaredField.setAccessible(true);
            Object castedValue = null;
            String fieldType = fieldMapping.getFieldType(); // Assuming fieldType is stored as a String
            switch (fieldType.toLowerCase()) {
                case "string":
                    castedValue = String.valueOf(columnValue);
                    break;
                case "int":
                    castedValue = Integer.parseInt(String.valueOf(columnValue));
                    break;
                case "double":
                    castedValue = Double.parseDouble(String.valueOf(columnValue));
                    break;
                // Add more cases for other data types as needed
                default:
                    // Handle unsupported data types or unknown field types
                    break;
            }
            declaredField.set(builder, castedValue);

        }
        return (T) builder.getClass().getMethod("build").invoke(builder);
    }

    @Override
    public T call(Row row) throws Exception {
        return parseObjectFromRow(row);
    }
}
