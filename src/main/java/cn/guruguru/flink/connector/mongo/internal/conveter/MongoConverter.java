package cn.guruguru.flink.connector.mongo.internal.conveter;

import org.apache.flink.annotation.Internal;
import org.bson.BsonDocument;

import java.io.Serializable;

/**
 * A converter used to converts the input record into MongoDB {@link BsonDocument},
 * and converts MongoDB {@link BsonDocument} into interval data structure.
 */
@Internal
public interface MongoConverter extends Serializable {
}
