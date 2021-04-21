package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public interface MgSerializationConverter<T> extends MgConverter {
    /**
     * Converts the input record into MongoDB {@link BsonDocument}.
     */
    BsonDocument toExternal(T data) throws MongoTypeConversionException;

    BsonDocument toExternal(T data, BsonDocument bsonDocument) throws MongoTypeConversionException;
}
