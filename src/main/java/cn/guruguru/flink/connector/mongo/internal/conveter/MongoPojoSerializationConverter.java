package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

/**
 * TODO
 */
public class MongoPojoSerializationConverter<T> implements MgSerializationConverter<T> {

    @Override
    public BsonDocument toExternal(Object data, BsonDocument doc) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public BsonDocument toExternal(T data, String[] fields, BsonDocument doc) throws MongoTypeConversionException {
        return null;
    }
}
