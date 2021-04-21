package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public class MongoPojoSerializationConverter<T> implements MgSerializationConverter {
    @Override
    public BsonDocument toExternal(Object data) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public BsonDocument toExternal(Object data, BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }
}
