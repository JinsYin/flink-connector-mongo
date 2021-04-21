package cn.guruguru.flink.connector.mongo.sink;

import com.mongodb.MongoException;

public class MongoSinkException extends MongoException {

    public MongoSinkException(String msg) {
        this(msg, null);
    }

    public MongoSinkException(String msg, Throwable t) {
        super(msg, t);
    }
}
