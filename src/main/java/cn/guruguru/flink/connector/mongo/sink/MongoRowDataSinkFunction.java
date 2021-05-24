package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.internal.connection.DefaultMongoClientFactory;
import cn.guruguru.flink.connector.mongo.internal.conveter.MgSerializationConverter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class MongoRowDataSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoRowDataSinkFunction.class);

    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();

    private final MgSerializationConverter<RowData> mongoSerConverter; // MongoSerializationConverter is serializable
    private final String[] keyNames;
    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private final long maxRetries;
    private final int batchSize;
    private final long batchIntervalMs;
    private final boolean ordered;

    private List<BsonDocument> batch = new ArrayList<>();
    private int batchCount = 0;

    private transient MongoClient mgClient;
    private transient MongoCollection<BsonDocument> mgCollection;

    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;

    public MongoRowDataSinkFunction(
            MgSerializationConverter<RowData> mongoSerConverter,
            String[] keyNames,
            String uri,
            String databaseName,
            String collectionName,
            long maxRetries,
            int batchSize,
            long batchIntervalMs,
            boolean ordered) {
        this.mongoSerConverter = mongoSerConverter;
        this.keyNames = keyNames;
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.ordered = ordered;
    }

    // --------------- AbstractRichFunction ---------------

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.debug("Starting MongoDB connection");

        mgClient = new DefaultMongoClientFactory(uri).create();
        mgCollection = mgClient
                .getDatabase(databaseName)
                .getCollection(collectionName, BsonDocument.class);

        LOG.debug("Started MongoDB connection");
    }

    // --------------- RichSinkFunction ---------------

    /**
     * Save data to MongoDB
     */
    @Override
    public void invoke(RowData row, Context context) {
        // Convert the RowData to the BsonDocument
        BsonDocument doc = mongoSerConverter.toExternal(row);

        if (keyNames.length == 0) {
            // insert
            mgCollection.insertOne(doc);
            // Bulk write
            //flush(bsonDocument);
        } else {
            // upsert
            replaceOne(row, keyNames);
        }
    }

    /**
     * Replace data when filter conditions are met
     */
    private void replaceOne(RowData rowData, String[] keyNames) {
        // 不需要 keyNames -> keyTypes -> keyConverter，因为需要替换所以会有一个全集
        // 但是 Lookup 需要这么做，参考 JDBC JdbcRowDataLookupFunction
        BsonDocument replacement = mongoSerConverter.toExternal(rowData);
        BsonDocument filter = new BsonDocument();
        mongoSerConverter.toExternal(rowData, keyNames, filter);
        mgCollection.replaceOne(filter, replacement);
    }

    private void flush(BsonDocument bsonDocument) throws MongoSinkException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                addToBatch(bsonDocument);
                batchCount++;
                if (batchCount % batchSize == 0) { // interval time ?
                    executeBatch(batch);
                    batchCount = 0;
                    batch = new ArrayList<>();
                }
                break;
            } catch (Exception e) {
                LOG.error("MongoDB insertMany error, retry times = {}", i, e);
                if (i >= maxRetries) {
                    throw new MongoSinkException("Mongo retry error");
                }
                // retry connect mongodb
            }
        }
    }

    private void addToBatch(BsonDocument bsonDocument) {
        batch.add(bsonDocument);
    }

    /**
     * see cn.guruguru.flink.connector.jdbc.internal.JdbcBatchingOutputFormat
     * @see com.mongodb.client.model.InsertManyOptions
     * @see BulkWriteOptions
     * @see <a href="https://stackoverflow.com/questions/35758690/mongodb-insertmany-vs-bulkwrite">InsertMany vs BulkWrite</a>
     */
    private void executeBatch(List<BsonDocument> bsonDocumentList) {
        LOG.debug("Bulk writing {} document(s) into collection [{}]",
                batchSize,
                    mgCollection);
        InsertManyOptions insertManyOptions = new InsertManyOptions().ordered(this.ordered);
        InsertManyResult result = mgCollection.insertMany(bsonDocumentList, insertManyOptions);
        LOG.debug("MongoDB bulk write result: {}", result);
    }

    // --------------- AbstractRichFunction ---------------

    @Override
    public void close() {
        if (mgClient != null) {
            try {
                mgClient.close();
            } catch (Exception e) {
                LOG.error("MongoDB client close exception.");
            }
            this.mgClient = null;
        }
    }

    @Override
    public String toString() {
        return "MongoDB";
    }

    // --------------- CheckpointedFunction ---------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
