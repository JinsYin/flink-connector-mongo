package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.api.java.tuple.Tuple;
import org.bson.BsonDocument;

/**
 * TODO
 */
public class MongoTupleSerializationConverter implements MgSerializationConverter<Tuple> {

    @Override
    public BsonDocument toExternal(Tuple data, BsonDocument doc) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public BsonDocument toExternal(Tuple data, String[] fields, BsonDocument doc) throws MongoTypeConversionException {
        return null;
    }
}
