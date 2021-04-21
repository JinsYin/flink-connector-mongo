package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public interface MgDeserializationConverter<T> extends MgConverter {
    /**
     * Converts MongoDB {@link BsonDocument} into interval data structure
     */
    T toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException;

    T toInternal(T t, BsonDocument bsonDocument) throws MongoTypeConversionException;
}
