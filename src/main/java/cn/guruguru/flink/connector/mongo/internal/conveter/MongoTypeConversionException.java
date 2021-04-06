package cn.guruguru.flink.connector.mongo.internal.conveter;

import com.mongodb.MongoException;

public class MongoTypeConversionException extends MongoException {

    public MongoTypeConversionException(String msg) {
        this(msg, null);
    }

    public MongoTypeConversionException(String msg, Throwable t) {
        super(msg, t);
    }
}
