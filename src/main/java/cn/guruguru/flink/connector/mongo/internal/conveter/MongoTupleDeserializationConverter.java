package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.bson.BsonDocument;

public class MongoTupleDeserializationConverter implements MgDeserializationConverter {

    @Override
    public Object toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public Object toInternal(Object o, BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }
}
