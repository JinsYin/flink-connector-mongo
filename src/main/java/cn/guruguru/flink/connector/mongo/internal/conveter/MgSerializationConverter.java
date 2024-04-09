package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public interface MgSerializationConverter<T> extends MgConverter {

    /**
     * Converts record into MongoDB {@link BsonDocument}.
     *
     * @param data a record
     * @param doc  a bson document
     * @return a bson document
     */
    BsonDocument toExternal(T data, BsonDocument doc) throws MongoTypeConversionException;

    /**
     * Converts record into MongoDB {@link BsonDocument}.
     *
     * @param data a record
     * @param fields field list
     * @param doc a bson document
     * @return a bson document
     */
    BsonDocument toExternal(T data, String[] fields, BsonDocument doc) throws MongoTypeConversionException;

    default BsonDocument toExternal(T data) throws MongoTypeConversionException {
        BsonDocument doc = new BsonDocument();
        return toExternal(data, doc);
    }

    default BsonDocument toExternal(T data, String[] fields) throws MongoTypeConversionException {
        BsonDocument doc = new BsonDocument();
        return toExternal(data, fields, doc);
    }

}
