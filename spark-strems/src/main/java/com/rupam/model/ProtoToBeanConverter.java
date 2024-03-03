package com.rupam.model;

import com.google.protobuf.GeneratedMessageV3;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

@Component
public class ProtoToBeanConverter {

    public <T> T convert(GeneratedMessageV3 source, Class<T> targetType) {
        try {
            T bean = targetType.getDeclaredConstructor().newInstance();
            Field[] fields = targetType.getDeclaredFields();
            for (Field field : fields) {
                String fieldName = field.getName();
                try {
                    Field protoField = source.getClass().getDeclaredField(fieldName);
                    protoField.setAccessible(true);
                    Object value = protoField.get(source);
                    field.setAccessible(true);
                    field.set(bean, value);
                } catch (NoSuchFieldException | IllegalAccessException ignored) {
                    // Field not found in Protobuf or unable to access, continue to the next field
                }
            }
            return bean;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace(); // Handle or log exception as needed
            return null;
        }
    }
}
