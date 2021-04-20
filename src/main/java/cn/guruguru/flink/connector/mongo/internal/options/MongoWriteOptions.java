package cn.guruguru.flink.connector.mongo.internal.options;

import java.io.Serializable;

public class MongoWriteOptions implements Serializable {

    /**
     * @see com.mongodb.client.model.BulkWriteOptions#ordered(boolean)
     * @see com.mongodb.client.model.InsertManyOptions#ordered(boolean)
     */
    public static final boolean DEFAULT_ORDERED = true;
    public static final long DEFAULT_MAX_RETRIES = 3;

    // ~ instance fields -----------------------------------

    private final long maxRetries;
    private final int batchSize;    // batch size
    private final long batchIntervalMs; // batch interval in millisecond
    private final boolean ordered;

    // ~ constructors --------------------------------------

    public MongoWriteOptions(long maxRetries, int batchSize, long batchIntervalMs, boolean ordered) {
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.ordered = ordered;
    }

    // ~ getters -------------------------------------------

    public long getMaxRetries() {
        return maxRetries;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public boolean getOrdered() {
        return ordered;
    }

    // ~ methods ------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    // ~ Builder -------------------------------------------

    /**
     * Builder for {@link MongoWriteOptions}
     */
    public static final class Builder {
        private long maxRetries = DEFAULT_MAX_RETRIES;
        private int batchSize;
        private long batchIntervalMs;
        private boolean ordered = DEFAULT_ORDERED;

        /**
         * optional, max retries
         */
        public Builder setMaxRetries(long maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * optional, max batch size
         */
        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * optional, batch interval in millisecond
         */
        public Builder setBatchIntervalMs(long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        /**
         * optional
         *
         * @see com.mongodb.client.model.BulkWriteOptions#ordered(boolean)
         * @see com.mongodb.client.model.InsertManyOptions#ordered(boolean)
         */
        public Builder setOrdered(boolean ordered) {
            this.ordered = ordered;
            return this;
        }

        public MongoWriteOptions build() {
            return new MongoWriteOptions(maxRetries, batchSize, batchIntervalMs, ordered);
        }
    }
}
