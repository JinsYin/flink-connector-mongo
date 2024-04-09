package cn.guruguru.flink.connector.mongo.internal.connection;

import com.mongodb.client.MongoClient;

import java.io.Serializable;

/**
 * A factory for creating MongoClients
 */
public interface MongoClientFactory extends Serializable {

    /**
     * Creates a `MongoClient`
     *
     * @return a {@link MongoClient}
     */
    MongoClient create();
}
