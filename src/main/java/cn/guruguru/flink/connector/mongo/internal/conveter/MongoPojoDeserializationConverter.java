package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

/**
 * TODO
 */
public class MongoPojoDeserializationConverter<T> implements MgDeserializationConverter<T> {

    @Override
    public T toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public T toInternal(BsonDocument bsonDocument, T data) throws MongoTypeConversionException {
        return null;
    }
}
