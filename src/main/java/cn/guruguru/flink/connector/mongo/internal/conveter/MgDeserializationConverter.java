package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public interface MgDeserializationConverter<T> extends MgConverter {
    /**
     * Converts MongoDB {@link BsonDocument} into interval data structure
     *
     * @param bsonDocument a bson document
     * @return a target object
     */
    T toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException;

    /**
     * Converts MongoDB {@link BsonDocument} into interval data structure
     *
     * @param bsonDocument a bson document
     * @param data interval object
     * @return a target object
     */
    T toInternal(BsonDocument bsonDocument, T data) throws MongoTypeConversionException;
}
