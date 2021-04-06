package cn.guruguru.flink.connector.mongo.internal.options;

import java.io.Serializable;

public class MongoReadOptions implements Serializable {

    public static final long serialVersionUID = 1L;
    public static final int DEFAULT_FETCH_SIZE = 0;
    public static final boolean DEFAULT_EXCLUDE_ID = true;

    // ~ instance fields --------------------------

    private final int fetchSize;
    private final boolean excludeId;

    // ~ constructors -----------------------------

    public MongoReadOptions(int fetchSize, boolean excludeId) {
        this.fetchSize = fetchSize;
        this.excludeId = excludeId;
    }

    // ~ getters ----------------------------------

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean isExcludeId() {
        return excludeId;
    }

    // ~ methods ----------------------------------

    public static Builder builder() {
        return new Builder();
    }

    // ~ Builder ----------------------------------

    /**
     * Builder for {@link MongoReadOptions}
     */
    public static class Builder {
        private int fetchSize = DEFAULT_FETCH_SIZE;
        private boolean excludeId = DEFAULT_EXCLUDE_ID;

        public Builder setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder setExcludeId(boolean excludeId) {
            this.excludeId = excludeId;
            return this;
        }

        public MongoReadOptions build() {
            return new MongoReadOptions(fetchSize, excludeId);
        }
    }
}
