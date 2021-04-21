package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.api.java.tuple.Tuple;
import org.bson.BsonDocument;

/**
 * TODO
 */
public class MongoTupleSerializationConverter implements MgSerializationConverter<Tuple> {

    @Override
    public BsonDocument toExternal(Tuple data) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public BsonDocument toExternal(Tuple data, BsonDocument bsonDocument) throws MongoTypeConversionException {
        return null;
    }
}
