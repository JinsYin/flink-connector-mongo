package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.internal.connection.DefaultMongoClientFactory;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoSerializationConverter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class MongoRowDataSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoRowDataSinkFunction.class);

    private final MongoSerializationConverter<RowData> mongoConverter; // MongoConverter is serializable
    private final String uri;
    private final String databaseName;
    private final String collectionName;
    private final long bufferFlushMaxRows;
    private final long bufferFlushIntervalMs;

    private transient MongoClient mgClient;
    private transient MongoCollection<BsonDocument> mgCollection;

    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();

    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;

    public MongoRowDataSinkFunction(
            MongoSerializationConverter<RowData> mongoConverter,
            String uri,
            String databaseName,
            String collectionName,
            long bufferFlushMaxRows,
            long bufferFlushIntervalMs) {
        this.mongoConverter = mongoConverter;
        this.uri = uri;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.bufferFlushMaxRows = bufferFlushMaxRows;
        this.bufferFlushIntervalMs = bufferFlushIntervalMs;
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
     */
    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        BsonDocument bsonDocument = new BsonDocument();
        mongoConverter.toExternal(rowData, bsonDocument);

        mgCollection.insertOne(bsonDocument);

        /**
         * {@link com.mongodb.client.model.InsertManyOptions}
         */
        //mgCollection.insertMany

        /**
         * {@link BulkWriteOptions}
         */
//        LOG.debug(
//                "Bulk writing {} document(s) into collection [{}]",
//                writeModels.size(),
//                collection);
//
//        BulkWriteResult result = getMongoClient()
//                        .getDatabase(database)
//                        .getCollection(collection, BsonDocument.class)
//                        .bulkWrite(writeModels, BULK_WRITE_OPTIONS);
//        LOG.debug("Mongodb bulk write result: {}", result);
    }

    // --------------- AbstractRichFunction ---------------

    @Override
    public void close() throws Exception {
        if (mgClient != null) {
            mgClient.close();
            this.mgClient = null;
        }
    }

    @Override
    public String toString() {
        return "123";
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
