package cn.guruguru.flink.connector.mongo.internal.options;

import java.io.Serializable;
import java.time.Duration;

public class MongoLookupOptions implements Serializable {

    public static final long serialVersionUID = 1L;

    public static final int DEFAULT_CACHE_MAX_ROWS = -1;
    public static final Duration DEFAULT_CACHE_TTL = Duration.ofSeconds(10);
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final boolean DEFAULT_EXCLUDE_ID = true;

    // ~ instance fields ---------------------------------

    private final int cacheMaxRows;
    private final Duration cacheTtl;
    private final int maxRetries;
    private final boolean excludeId;

    // ~ constructor --------------------------------------

    public MongoLookupOptions(int cacheMaxRows, Duration cacheTtl, int maxRetries, boolean excludeId) {
        this.cacheMaxRows = cacheMaxRows;
        this.cacheTtl = cacheTtl;
        this.maxRetries = maxRetries;
        this.excludeId = excludeId;
    }

    // ~ getters ------------------------------------------

    public int getCacheMaxRows() {
        return cacheMaxRows;
    }

    public Duration getCacheTtl() {
        return cacheTtl;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public boolean isExcludeId() {
        return excludeId;
    }

    // ~ methods ------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    // ~ Builder ------------------------------------------

    /**
     * Builder for {@link MongoLookupOptions}
     */
    public static class Builder {
        private int cacheMaxRows = DEFAULT_CACHE_MAX_ROWS;
        private Duration cacheTtl = DEFAULT_CACHE_TTL;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private boolean excludeId = DEFAULT_EXCLUDE_ID;

        public Builder setCacheMaxRows(int cacheMaxRows) {
            this.cacheMaxRows = cacheMaxRows;
            return this;
        }

        public Builder setCacheTtl(Duration cacheTtl) {
            this.cacheTtl = cacheTtl;
            return this;
        }

        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }
        
        public Builder setExcludeId(boolean excludeId) {
            this.excludeId = excludeId;
            return this;
        }

        public MongoLookupOptions build() {
            return new MongoLookupOptions(cacheMaxRows, cacheTtl, maxRetries, excludeId);
        }
    }
}
