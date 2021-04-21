package cn.guruguru.flink.connector.mongo.source;

import cn.guruguru.flink.connector.mongo.internal.connection.DefaultMongoClientFactory;
import cn.guruguru.flink.connector.mongo.internal.conveter.MgDeserializationConverter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.bson.BsonDocument;

import static com.mongodb.client.model.Projections.exclude;

/**
 *
 * The {@link RichSourceFunction} extends {@link AbstractRichFunction} class
 * and implements {@link org.apache.flink.streaming.api.functions.source.SourceFunction} interface.
 *
 * @see cn.guruguru.flink.connector.mongo.internal.options.MongoReadOptions
 * @see org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource
 */
public class MongoRowDataSourceFunction<RowData> extends RichSourceFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final MgDeserializationConverter<RowData> deserConverter; // MongoConverter is serializable
    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private final int fetchSize;
    private final boolean excludeId;

    private transient MongoClient mongoClient;
    private transient MongoCollection<BsonDocument> mongoCollection;

    public MongoRowDataSourceFunction(
            MgDeserializationConverter<RowData> deserConverter,
            String uri,
            String databaseName,
            String collectionName,
            int fetchSize,
            boolean excludeId) {
        this.deserConverter = deserConverter;
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.fetchSize = fetchSize;
        this.excludeId = excludeId;
    }

    // --------------------------------------------------
    // AbstractRichFunction
    // --------------------------------------------------

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        mongoClient = new DefaultMongoClientFactory(uri).create();
        mongoCollection = mongoClient
                .getDatabase(databaseName)
                .getCollection(collectionName, BsonDocument.class);
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    // --------------------------------------------------
    // SourceFunction
    // --------------------------------------------------

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        if (excludeId) {
            try (MongoCursor<BsonDocument> cursor = mongoCollection.find().projection(exclude("_id"))
                    .batchSize(fetchSize).iterator()) {
                while (cursor.hasNext()) {
                    BsonDocument bsonDocument = cursor.next();
                    RowData rowData = deserConverter.toInternal(bsonDocument);
                    ctx.collect(rowData);
                }
            }
        } else {
            try (MongoCursor<BsonDocument> cursor = mongoCollection.find().batchSize(fetchSize).iterator()) {
                while (cursor.hasNext()) {
                    BsonDocument bsonDocument = cursor.next();
                    RowData rowData = deserConverter.toInternal(bsonDocument);
                    ctx.collect(rowData);
                }
            }
        }
    }

    @Override
    public void cancel() {

    }

    // --------------------------------------------------
    // CheckpointedFunction
    // --------------------------------------------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
