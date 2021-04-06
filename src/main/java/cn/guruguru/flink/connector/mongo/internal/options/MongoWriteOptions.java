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
    private final long bufferFlushMaxRows;    // batch size
    private final long bufferFlushIntervalMs; // batch interval in millisecond
    private final boolean ordered;

    // ~ constructors --------------------------------------

    public MongoWriteOptions(long maxRetries, long bufferFlushMaxRows, long bufferFlushIntervalMs, boolean ordered) {
        this.maxRetries = maxRetries;
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        this.bufferFlushIntervalMs = bufferFlushIntervalMs;
        this.ordered = ordered;
    }

    // ~ getters -------------------------------------------

    public long getMaxRetries() {
        return maxRetries;
    }

    public long getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public long getBufferFlushIntervalMs() {
        return bufferFlushIntervalMs;
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
        private long bufferFlushMaxRows;
        private long bufferFlushIntervalMs;
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
        public Builder setBufferFlushMaxRows(long bufferFlushMaxRows) {
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        /**
         * optional, batch interval in millisecond
         */
        public Builder setBufferFlushMaxIntervalMs(long bufferFlushIntervalMs) {
            this.bufferFlushIntervalMs = bufferFlushIntervalMs;
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
            return new MongoWriteOptions(maxRetries, bufferFlushMaxRows, bufferFlushIntervalMs, ordered);
        }
    }
}
