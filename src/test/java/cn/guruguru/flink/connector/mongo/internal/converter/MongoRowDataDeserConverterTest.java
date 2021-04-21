package cn.guruguru.flink.connector.mongo.internal.converter;

import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataDeserializationConverter;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.junit.Test;

/**
 * Unit Test for {@link MongoRowDataDeserializationConverter}
 */
public class MongoRowDataDeserConverterTest {
    @Test
    public void testToInternal() {
        BsonDocument expected = new BsonDocument();
        expected.append("a", new BsonDouble(1.2f));
    }

    /**
     * TODO
     */
    @Test
    public void testUnsupportedType() {}
}
