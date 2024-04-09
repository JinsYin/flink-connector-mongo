package cn.guruguru.flink.connector.mongo.table;

import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataSerializationConverter;
import cn.guruguru.flink.connector.mongo.internal.options.MongoOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoWriteOptions;
import cn.guruguru.flink.connector.mongo.sink.MongoRowDataSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mongodb dynamic table for writing.
 *
 * @see org.apache.flink.table.factories.PrintTableSinkFactory
 * @author JinsYin
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
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataSerializationConverter serConverter = new MongoRowDataSerializationConverter(rowType);
        context.createDataStructureConverter(tableSchema.toPhysicalRowDataType());

        List<String> keyNameList = new ArrayList<>();
        tableSchema.getPrimaryKey().ifPresent(key -> key.getColumns().stream().collect(Collectors.toCollection(() -> keyNameList)));

        MongoRowDataSinkFunction sinkFunction = new MongoRowDataSinkFunction(
                serConverter,
                keyNameList.toArray(new String[]{}),
                mongoOptions.getUri(),
                mongoOptions.getDatabaseName(),
                mongoOptions.getCollectionName(),
                writeOptions.getMaxRetries(),
                writeOptions.getBatchSize(),
                writeOptions.getBatchIntervalMs(),
                writeOptions.getOrdered());
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
