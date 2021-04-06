package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.api.java.tuple.Tuple;
import org.bson.BsonDocument;

/**
 * TODO
 */
public class TupleMongoConverter implements
        MongoSerializationConverter<Tuple>,
        MongoDeserializationConverter<Tuple> {

    @Override
    public BsonDocument toExternal(Tuple tuple, BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public Tuple toInternal(BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }
}
