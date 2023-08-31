package com.rupam.service.writes;

import org.apache.spark.sql.ForeachWriter;

public class CustomForEachWriter<T> extends ForeachWriter {

    @Override
    public boolean open(long partitionId, long epochId) {
        return false;
    }

    @Override
    public void process(Object value) {

        System.out.println(value);
    }

    @Override
    public void close(Throwable errorOrNull) {

    }
}
