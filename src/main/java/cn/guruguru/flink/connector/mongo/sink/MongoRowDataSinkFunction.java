package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.internal.connection.DefaultMongoClientFactory;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoSerializationConverter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.result.InsertManyResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class MongoRowDataSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoRowDataSinkFunction.class);

    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();

    private final MongoSerializationConverter<RowData> mongoConverter; // MongoConverter is serializable
    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private final long maxRetries;
    private final int batchSize;
    private final long batchIntervalMs;

    private List<BsonDocument> batch = new ArrayList<>();
    private int batchCount = 0;

    private transient MongoClient mgClient;
    private transient MongoCollection<BsonDocument> mgCollection;

    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;

    public MongoRowDataSinkFunction(
            MongoSerializationConverter<RowData> mongoConverter,
            String uri,
            String databaseName,
            String collectionName,
            long maxRetries,
            int batchSize,
            long batchIntervalMs) {
        this.mongoConverter = mongoConverter;
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    // --------------- AbstractRichFunction ---------------

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("Starting MongoDB connection");

        mgClient = new DefaultMongoClientFactory(uri).create();
        mgCollection = mgClient
                .getDatabase(databaseName)
                .getCollection(collectionName, BsonDocument.class);

        LOG.info("Started MongoDB connection");
    }

    // --------------- RichSinkFunction ---------------

    /**
     * Save data to MongoDB
     *
     * @see com.mongodb.client.model.InsertManyOptions
     * @see BulkWriteOptions
     * @see cn.guruguru.flink.connector.jdbc.internal.JdbcBatchingOutputFormat
     * @see <a href="https://stackoverflow.com/questions/35758690/mongodb-insertmany-vs-bulkwrite">InsertMany vs BulkWrite</a>
     */
    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        BsonDocument bsonDocument = new BsonDocument();
        // Convert the RowData to the BsonDocument
        bsonDocument = mongoConverter.toExternal(rowData, bsonDocument);

        // Insert one
        mgCollection.insertOne(bsonDocument);

        // Bulk write
//        for (int i = 0; i <= maxRetries; i++) {
//            try {
//                addToBatch(bsonDocument);
//                batchCount++;
//                if (batchCount % batchSize == 0) { // interval time ?
//                    executeBatch(batch);
//                    batchCount = 0;
//                    batch = new ArrayList<>();
//                }
//                break;
//            } catch (Exception e) {
//                LOG.error("MongoDB insertMany error, retry times = {}", i, e);
//                if (i >= maxRetries) {
//                    throw new IOException(e);
//                }
//                // retry connect mongodb
//            }
//        }
    }

    private void addToBatch(BsonDocument bsonDocument) {
        batch.add(bsonDocument);
    }

    private void executeBatch(List<BsonDocument> bsonDocumentList) {
        LOG.debug("Bulk writing {} document(s) into collection [{}]",
                batchSize,
                    mgCollection);
        InsertManyResult result = mgCollection.insertMany(bsonDocumentList);
        LOG.debug("Mongodb bulk write result: {}", result);
    }

    // --------------- AbstractRichFunction ---------------

    @Override
    public void close() {
        if (mgClient != null) {
            try {
                mgClient.close();
            } catch (Exception e) {
                LOG.error("close exception.");
            }
            this.mgClient = null;
        }
    }

    @Override
    public String toString() {
        return "Mongodb";
    }

    // --------------- CheckpointedFunction ---------------

//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//
//    }
}
