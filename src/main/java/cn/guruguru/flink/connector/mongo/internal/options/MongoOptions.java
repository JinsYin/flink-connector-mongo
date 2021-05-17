package cn.guruguru.flink.connector.mongo.internal.options;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MongoOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_MONGO_URI = "mongodb://127.0.0.1:27017";
    public static final long DEFAULT_LOCAL_THRESHOLD = MILLISECONDS.convert(15, MILLISECONDS);
    public static final long DEFAULT_CONNECTION_TIMEOUT = MILLISECONDS.convert(30, TimeUnit.SECONDS);

    private final String uri;
    private final String databaseName;
    private final String collectionName;

    public MongoOptions(String uri, String databaseName, String collectionName) {
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getUri() {
        return uri;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    // -------------------- Builder --------------------

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link MongoOptions}
     */
    public static final class Builder {
        private String uri = DEFAULT_MONGO_URI;
        private String databaseName;
        private String collectionName;

        /**
         * required, mongodb uri
         *
         * @see com.mongodb.ConnectionString
         */
        public Builder setUri(String uri) {
            this.uri = uri;
            return this;
        }

        /**
         * optional, database name
         */
        public Builder setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        /**
         * optional, collection name
         */
        public Builder setCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public MongoOptions build() {
            checkNotNull(uri, "MongoDB uri is not set.");
            return new MongoOptions(uri, databaseName, collectionName);
        }
    }
}
