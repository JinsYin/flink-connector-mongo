package cn.guruguru.flink.connector.mongo.internal.connection;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DefaultMongoClientFactory implements MongoClientFactory {

    private final String uri;
    private transient final MongoDriverInformation mongoDriverInformation;

    public DefaultMongoClientFactory(String uri) {
        this.uri = uri;
        this.mongoDriverInformation = MongoDriverInformation.builder().driverName("flink-connector-mongo")
                .driverPlatform("Java/1.8:Flink/1.11")
                .build();
    }

    @Override
    public MongoClient create() {
        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(connectionString);

        return MongoClients.create(builder.build(), mongoDriverInformation);
    }
}
