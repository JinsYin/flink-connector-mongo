package cn.guruguru.flink.connector.mongo.source;

import cn.guruguru.flink.connector.mongo.internal.connection.DefaultMongoClientFactory;
import cn.guruguru.flink.connector.mongo.internal.conveter.MgDeserializationConverter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.bson.BsonDocument;

import java.time.Duration;
import java.util.Arrays;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.exclude;

/**
 * Table Function which is used for Flink framework in runtime
 *
 * @see cn.guruguru.flink.connector.mongo.internal.options.MongoLookupOptions
 * @author JinsYin
 */
@Internal
public class MongoRowDataLookupFunction extends TableFunction<RowData> { // @Internal: RowData, @Public: Row

    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private final int cacheMaxRows;
    private final Duration cacheTtl;
    private final int maxRetries;
    private boolean excludeId = true;

    private final MgDeserializationConverter<RowData> deserConverter;

    private MongoClient mongoClient;
    private MongoCollection mongoCollection;

    public MongoRowDataLookupFunction(
            MgDeserializationConverter<RowData> deserConverter,
            String uri,
            String databaseName,
            String collectionName,
            int cacheMaxRows,
            Duration cacheTtl,
            int maxRetries,
            boolean excludeId) {
        this.deserConverter = deserConverter;
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.cacheMaxRows = cacheMaxRows;
        this.cacheTtl = cacheTtl;
        this.maxRetries = maxRetries;
        this.excludeId = excludeId;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        mongoClient = new DefaultMongoClientFactory(uri).create();
        mongoCollection = mongoClient
                .getDatabase(databaseName)
                .getCollection(collectionName, BsonDocument.class);
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     * @param keys lookup keys 只支持等值非嵌套的键
     */
    @SuppressWarnings("unchecked")
    public void eval(Object... keys) {
        String[] ks = (String[]) Arrays.stream(keys).toArray(String[]::new);
        if (excludeId) {
            try (MongoCursor<BsonDocument> cursor = mongoCollection.find(eq(ks)).projection(exclude("_id")).iterator()) {
                while (cursor.hasNext()) {
                    BsonDocument bsonDocument = cursor.next();
                    RowData rowData = deserConverter.toInternal(bsonDocument);
                    collect(rowData);
                }
            }
        } else {
            try (MongoCursor<BsonDocument> cursor = mongoCollection.find(eq(ks)).iterator()) {
                while (cursor.hasNext()) {
                    BsonDocument bsonDocument = cursor.next();
                    RowData rowData = deserConverter.toInternal(bsonDocument);
                    collect(rowData);
                }
            }
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
