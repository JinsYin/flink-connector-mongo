package cn.guruguru.flink.connector.mongo.table;

import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataDeserializationConverter;
import cn.guruguru.flink.connector.mongo.internal.options.MongoLookupOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoReadOptions;
import cn.guruguru.flink.connector.mongo.source.MongoRowDataLookupFunction;
import cn.guruguru.flink.connector.mongo.source.MongoRowDataSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * The LookupTableSource and ScanTableSource extend {@link DynamicTableSource}
 *
 * see org.apache.flink.table.factories.DataGenTableSourceFactory.DataGenTableSource
 *
 * @see org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource
 */
public class MongoDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private final MongoOptions mongoOptions;
    private final MongoReadOptions readOptions;
    private final MongoLookupOptions lookupOptions;

    private transient final TableSchema tableSchema; // TableSchema is not serializable

    public MongoDynamicTableSource(
            TableSchema tableSchema,
            MongoOptions mongoOptions,
            MongoReadOptions readOptions,
            MongoLookupOptions lookupOptions) {
        this.tableSchema = tableSchema;
        this.mongoOptions = mongoOptions;
        this.readOptions = readOptions;
        this.lookupOptions = lookupOptions;
    }

    // --------------------------------------------------
    // DynamicTableSource
    // --------------------------------------------------

    @Override
    public DynamicTableSource copy() {
        return new MongoDynamicTableSource(tableSchema, mongoOptions, readOptions, lookupOptions);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    // --------------------------------------------------
    // ScanTableSource
    // --------------------------------------------------

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataDeserializationConverter deserConverter = new MongoRowDataDeserializationConverter(rowType);
        MongoRowDataSourceFunction<RowData> sourceFunction = new MongoRowDataSourceFunction<>(
                deserConverter,
                mongoOptions.getUri(),
                mongoOptions.getDatabaseName(),
                mongoOptions.getCollectionName(),
                readOptions.getFetchSize(),
                readOptions.isExcludeId()
        );
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    // --------------------------------------------------
    // LookupTableSource
    // --------------------------------------------------

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataDeserializationConverter deserConverter = new MongoRowDataDeserializationConverter(rowType);

        MongoRowDataLookupFunction lookupFunction = new MongoRowDataLookupFunction(
                deserConverter,
                mongoOptions.getUri(),
                mongoOptions.getDatabaseName(),
                mongoOptions.getCollectionName(),
                lookupOptions.getCacheMaxRows(),
                lookupOptions.getCacheTtl(),
                lookupOptions.getMaxRetries(),
                lookupOptions.isExcludeId()
        );
        return TableFunctionProvider.of(lookupFunction);
    }

    // --------------------------------------------------
    // SupportsProjectionPushDown
    // --------------------------------------------------

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {

    }
}
