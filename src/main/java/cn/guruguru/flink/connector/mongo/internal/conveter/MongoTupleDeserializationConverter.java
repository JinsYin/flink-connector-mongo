package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.api.java.tuple.Tuple;
import org.bson.BsonDocument;

/**
 * TODO
 */
public class MongoTupleDeserializationConverter implements MgDeserializationConverter<Tuple> {

    @Override
    public Tuple toInternal(BsonDocument doc) throws MongoTypeConversionException {
        return null;
    }

    @Override
    public Tuple toInternal(BsonDocument doc, Tuple tuple) throws MongoTypeConversionException {
        return null;
    }
}
