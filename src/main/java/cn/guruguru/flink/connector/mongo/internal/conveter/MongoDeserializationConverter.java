package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public interface MongoDeserializationConverter<T> extends MongoConverter {
    /**
     * Converts MongoDB {@link BsonDocument} into interval data structure
     */
    T toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException;
}
