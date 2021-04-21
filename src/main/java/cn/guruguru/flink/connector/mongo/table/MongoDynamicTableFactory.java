package cn.guruguru.flink.connector.mongo.table;

import cn.guruguru.flink.connector.mongo.internal.options.MongoLookupOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoReadOptions;
import cn.guruguru.flink.connector.mongo.internal.options.MongoWriteOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkState;

/**
 * @see org.apache.flink.table.factories.DataGenTableSourceFactory
 * @see org.apache.flink.table.factories.PrintTableSinkFactory
 */
public class MongoDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "mongodb";

    // Connection options

    /**
     * @see com.mongodb.ConnectionString
     */
    public static final ConfigOption<String> URI = ConfigOptions
            .key("uri")
            .stringType()
            .defaultValue("mongodb://127.0.0.1:27017")
            .withDescription("MongoDB Connection URI");

    public static final ConfigOption<String> DATABASE = ConfigOptions
            .key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("Mongodb database name");

    public static final ConfigOption<String> COLLECTION = ConfigOptions
            .key("collection")
            .stringType()
            .noDefaultValue()
            .withDescription("Mongodb collection name");

    // Scan Options

    public static final ConfigOption<Integer> SCAN_FETCH_SIZE = ConfigOptions
            .key("scan.fetch-size")
            .intType()
            .defaultValue(0)
            .withDescription("The fetch size");

    public static final ConfigOption<Boolean> SCAN_EXCLUDE_ID = ConfigOptions
            .key("scan.exclude-id")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to exclude the ObjectId field when scanning");

    // Lookup Options

    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .intType()
            .defaultValue(10000)
            .withDescription("The cache max batch size");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions
            .key("lookup.cache.ttl")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("The cache time to live.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions
            .key("lookup.cache.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("The Max retry times if lookup database failed.");

    public static final ConfigOption<Boolean> LOOKUP_EXCLUDE_ID = ConfigOptions
            .key("lookup.exclude-id")
            .booleanType()
            .defaultValue(true)
            .withDescription("Whether to exclude the ObjectId field when looking up.");

    // Sink Options

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
            .key("sink.buffer-flush.max-rows")
            .intType()
            .defaultValue(1024)
            .withDescription("The flush max size (includes all append, upsert and delete records), over this number" +
                    " of records, will flush data. The default value is 100.");
    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.buffer-flush.interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1))
            .withDescription("The flush interval mills, over this time, asynchronous threads will flush data. The " +
                    "default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions
            .key("sink.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("The max retry times if writing records to database failed.");

    /**
     * @see com.mongodb.client.model.BulkWriteOptions#ordered(boolean)
     * @see com.mongodb.client.model.InsertManyOptions#ordered(boolean)
     */
    public static final ConfigOption<Boolean> SINK_ORDERED = ConfigOptions
            .key("sink.ordered")
            .booleanType()
            .defaultValue(true)
            .withDescription("ordered");

    // --------------- DynamicTableSourceFactory ---------------

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // 获取 WITH 选项（Configuration 继承了 ReadableConfig）
        final ReadableConfig config = helper.getOptions();

        // 验证 WITH 选项
        helper.validate();
        validateConfigOptions(config);

        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new MongoDynamicTableSource(
                tableSchema,
                getMongoOptions(config),
                getMongoReadOptions(config),
                getMongoLookupOptions(config)
        );
    }

    // --------------- DynamicTableSinkFactory ---------------

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // 获取 WITH 选项
        final ReadableConfig config = helper.getOptions();

        // 验证 WITH 选项
        helper.validate();
        validateConfigOptions(config);

        TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        //MongoTableSchema mongoTableSchema = MongoTableSchema.fromTableSchema(tableSchema); // TableSchema is not serializable

        // 要求每个参数对象必须可序列化
        return new MongoDynamicTableSink(
                tableSchema,
                getMongoOptions(config),
                getMongoWriteOptions(config));
    }

    // ~ methods ------------------------------------------------

    private MongoOptions getMongoOptions(ReadableConfig config) {
        final MongoOptions.Builder builder = MongoOptions.builder();
        builder.setUri(config.get(URI));
        // 针对没有默认值且又是可选的 WITH 选项
        config.getOptional(DATABASE).ifPresent(builder::setDatabaseName);
        config.getOptional(COLLECTION).ifPresent(builder::setCollectionName);
        return builder.build();
    }

    private MongoWriteOptions getMongoWriteOptions(ReadableConfig config) {
        return MongoWriteOptions.builder()
                .setMaxRetries(config.get(SINK_MAX_RETRIES))
                .setBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
                .setBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
                .build();
    }

    private MongoReadOptions getMongoReadOptions(ReadableConfig config) {
        return MongoReadOptions.builder()
                .setFetchSize(config.get(SCAN_FETCH_SIZE))
                .build();
    }

    private MongoLookupOptions getMongoLookupOptions(ReadableConfig config) {
        return MongoLookupOptions.builder()
                .setCacheMaxRows(config.get(LOOKUP_CACHE_MAX_ROWS))
                .setCacheTtl(config.get(LOOKUP_CACHE_TTL))
                .setCacheMaxRows(config.get(LOOKUP_CACHE_MAX_ROWS))
                .build();
    }

    // --------------- Factory ---------------

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    /**
     * 必填项
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URI);
        return requiredOptions;
    }

    /**
     * 选填项
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DATABASE);
        optionalOptions.add(COLLECTION);
        optionalOptions.add(SCAN_FETCH_SIZE);
        optionalOptions.add(SCAN_EXCLUDE_ID);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        optionalOptions.add(LOOKUP_EXCLUDE_ID);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        return optionalOptions;
    }

    // --------------- Utilities ---------------

    /**
     * 验证配置选项
     */
    private void validateConfigOptions(ReadableConfig config) {
        Optional<String> mongodbUri = config.getOptional(URI);
        checkState(mongodbUri.isPresent(), "Cannot handle such MongoDB uri: " + mongodbUri);

        // "lookup.cache.max-rows" 和 "lookup.cache.ttl" 选项要么两者都被指定，要么都不指定
        checkAllOrNone(config, new ConfigOption[]{
            LOOKUP_CACHE_MAX_ROWS,
            LOOKUP_CACHE_TTL
        });

        checkAllOrNone(config, new ConfigOption[]{
            DATABASE,
            COLLECTION
        });
    }

    /**
     * 要么都指定，要么都不指定
     */
    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
    }
}
