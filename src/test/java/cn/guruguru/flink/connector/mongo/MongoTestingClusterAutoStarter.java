package cn.guruguru.flink.connector.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.flink.test.util.AbstractTestBase;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.util.Collections;

/**
 * MongoDB server for integration tests.
 *
 * @see <a href="https://github.com/bwaldvogel/mongo-java-server">MongoDB Java Server</a>
 */
public class MongoTestingClusterAutoStarter extends AbstractTestBase {

    public static final String DEFAULT_DATABASE_NAME_FOR_IT = "testDatabase";
    public static final String DEFAULT_COLLECTION_NAME_FOR_IT = "testCollection";

    private static MongoServer mongoServer;
    private static MongoClient mongoClient;

    private static InetSocketAddress inetSocketAddress;

    @Before
    public void setUp() {
        mongoServer = new MongoServer(new MemoryBackend());

        // optionally:
        // server.enableSsl(key, keyPassword, certificate);
        // server.enableOplog();

        // bind on a certain local port
        // server.bind("localhost", 27017);

        // bind on a random local port
        inetSocketAddress = mongoServer.bind();

        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                    builder.hosts(Collections.singletonList(new ServerAddress(mongoServer.getLocalAddress()))))
                .build();
        mongoClient = MongoClients.create(mongoClientSettings);

    }

    @After
    public void tearDown() {
        mongoClient.close();
        mongoServer.shutdown();
    }

    // InetSocketAddress methods ----------------------------------

    protected static int getTestMongoPort() {
        return inetSocketAddress.getPort();
    }

    protected static String getTestMongoHost() {
        return inetSocketAddress.getHostName();
    }

    protected static String getTestMongoUri() {
        return String.format("mongodb://%s:%s", getTestMongoHost(), getTestMongoPort());
    }

    // MongoDB methods ---------------------------------------------

    protected static MongoClient getTestMongoClient() {
        return mongoClient;
    }

    protected static MongoDatabase getTestMongoDatabase() {
        return mongoClient.getDatabase(DEFAULT_DATABASE_NAME_FOR_IT);
    }

    protected static MongoCollection<BsonDocument> getTestMongoCollection() {
        return getTestMongoDatabase().getCollection(DEFAULT_COLLECTION_NAME_FOR_IT, BsonDocument.class); // not Document.class
    }

    protected static String getDefaultTestDatabaseName() {
        return DEFAULT_DATABASE_NAME_FOR_IT;
    }

    protected static String getDefaultTestCollectionName() {
        return DEFAULT_COLLECTION_NAME_FOR_IT;
    }

}
