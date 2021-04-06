package cn.guruguru.flink.connector.mongo.table;

import cn.guruguru.flink.connector.mongo.internal.conveter.RowDataMongoConverter;
import cn.guruguru.flink.connector.mongo.internal.options.MongoOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoWriteOptions;
import cn.guruguru.flink.connector.mongo.sink.MongoRowDataSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * see org.apache.flink.table.factories.PrintTableSinkFactory.PrintSink
 */
public class MongoDynamicTableSink implements DynamicTableSink {

    //private final MongoTableSchema mongoTableSchema; // TableSchema is not serializable
    private final MongoOptions mongoOptions;
    private final MongoWriteOptions writeOptions;

    private transient final TableSchema tableSchema;

    public MongoDynamicTableSink(
            TableSchema tableSchema,
            MongoOptions mongoOptions,
            MongoWriteOptions writeOptions) {
        this.tableSchema = tableSchema;
        this.mongoOptions = mongoOptions;
        this.writeOptions = writeOptions;
    }

    /**
     * 支持写入的行的类别
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        RowDataMongoConverter rowDataMongoConverter = new RowDataMongoConverter(rowType);

        context.createDataStructureConverter(tableSchema.toPhysicalRowDataType());

        MongoRowDataSinkFunction sinkFunction = new MongoRowDataSinkFunction(
                rowDataMongoConverter,
                mongoOptions.getUri(),
                mongoOptions.getDatabaseName(),
                mongoOptions.getCollectionName(),
                writeOptions.getBufferFlushMaxRows(),
                writeOptions.getBufferFlushIntervalMs());
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new MongoDynamicTableSink(tableSchema, mongoOptions, writeOptions);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }
}
