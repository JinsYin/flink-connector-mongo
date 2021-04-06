package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

/**
 * TODO
 */
public class PojoMongoConverter<T> implements
        MongoSerializationConverter<T>,
        MongoDeserializationConverter<T> {

    @Override
    public BsonDocument toExternal(T record, BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public T toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }
}
