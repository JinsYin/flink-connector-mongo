package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public interface MongoSerializationConverter<T> extends MongoConverter {
    /**
     * Converts the input record into MongoDB {@link BsonDocument}.
     */
    BsonDocument toExternal(T data, BsonDocument bsonDocument) throws MongoTypeConversionException;
}
