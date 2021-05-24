package cn.guruguru.flink.connector.mongo.table;

import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataDeserializationConverter;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataSerializationConverter;
import cn.guruguru.flink.connector.mongo.sink.MongoRowDataSinkFunction;
import cn.guruguru.flink.connector.mongo.source.MongoRowDataSourceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.fields;
import static org.junit.Assert.assertEquals;

/**
 * End to end tests for {@link MongoDynamicTableFactory}.
 *
 * @see org.apache.flink.table.planner.runtime.stream.table.PrintConnectorITCase
 */
public class MongoConnectorITCase extends MongoTestingClusterAutoStarter {

    public static String TEST_DATABASE = "testDatabase";
    public static String TEST_COLLECTION_1 = "testCollection1";
    public static String TEST_COLLECTION_2 = "testCollection2";
    public static String TEST_COLLECTION_3 = "testCollection3";

    public static String TEST_TABLE_1 = "testTable1";
    public static String TEST_TABLE_2 = "testTable2";

    // --------------------------------------------------------
    // Table connector integration test
    // --------------------------------------------------------

    @Test
    public void testDynamicTableSource() {
        getTestMongoClient().getDatabase(TEST_DATABASE).getCollection(TEST_COLLECTION_1, BsonDocument.class);
    }

    @Test
    public void testSqlSelect() {
        // 欲写入记录
        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            BsonDocument document = new BsonDocument()
                    .append("a", new BsonInt32(i))
                    .append("b", new BsonInt32(i * 10))
                    .append("c", new BsonInt32(i * 100))
                    .append("d", new BsonInt32(i * 1000));
            documents.add(document);
        }
        getTestMongoCollection().insertMany(documents);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String table1DDL = "CREATE TABLE " + TEST_TABLE_1  + "(\n" +
                "    a INT,\n" +
                "    b INT,\n" +
                "    c INT,\n" +
                "    d INT\n" +
                ") WITH (\n" +
                "    'connector' = 'mongodb',\n" +
                "    'uri' = '" + getTestMongoUri() + "',\n" +
                "    'database' = '" + getDefaultTestCollectionName() + "',\n" +
                "    'collection' = '" + getDefaultTestCollectionName() + "'\n" +
                ")";
        tEnv.executeSql(table1DDL);

        String query = "SELECT a, b, c, d FROM " + TEST_TABLE_1;
        Iterator<Row> collected = tEnv.executeSql(query).collect();
        List<String> result = Lists.newArrayList(collected).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());

        result.forEach(System.out::println);
    }

    @Test
    public void testDynamicTableLookupSource() throws Exception {
        // 欲写入记录
        List<BsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            BsonDocument document = new BsonDocument()
                    .append("a", new BsonInt32(i))
                    .append("b", new BsonInt32(i * 10))
                    .append("c", new BsonInt32(i * 100))
                    .append("d", new BsonInt32(i * 1000));
            documents.add(document);
        }
        getTestMongoCollection().insertMany(documents);

        // 表结构
        TableSchema tableSchema = TableSchema.builder()
                .field("a", DataTypes.STRING().notNull())
                .field("b", DataTypes.BOOLEAN())
                .field("c", DataTypes.INT().notNull())
                .build();

        // connector options
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb");
        options.put(MongoDynamicTableFactory.URI.key(), getTestMongoUri());
        options.put(MongoDynamicTableFactory.DATABASE.key(), DEFAULT_DATABASE_NAME_FOR_IT);
        options.put(MongoDynamicTableFactory.COLLECTION.key(), DEFAULT_COLLECTION_NAME_FOR_IT);
        options.put(MongoDynamicTableFactory.LOOKUP_CACHE_MAX_ROWS.key(), "3");
        options.put(MongoDynamicTableFactory.LOOKUP_CACHE_TTL.key(), "3");
        options.put(MongoDynamicTableFactory.LOOKUP_MAX_RETRIES.key(), "3");

        MongoDynamicTableSource dynamicTableSource = (MongoDynamicTableSource) createMongoDynamicTableSource(tableSchema, options);
        ScanTableSource.ScanContext mockScanContext = new MockDynamicTableScanText();
        SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) dynamicTableSource.getScanRuntimeProvider(mockScanContext);

        SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();

        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataDeserializationConverter deserConverter = new MongoRowDataDeserializationConverter(rowType);
        MongoRowDataSourceFunction<RowData> sourceFunction1 = new MongoRowDataSourceFunction<RowData>(
                deserConverter,
                getTestMongoUri(),
                getDefaultTestDatabaseName(),
                getDefaultTestCollectionName(),
                3,
                false
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> dataStreamSource = env.addSource(sourceFunction1);

//        dataStreamSource.returns(Types.ROW(Types.STRING)).print();

//        List<RowData> actual = new ArrayList<>();
//        DataStreamUtils.collect(dataStreamSource).forEachRemaining(actual::add);
//
//        actual.stream().forEach(System.out::println);

        env.execute();
    }

    @Test
    public void testDynamicTableSink() throws Exception {
        // ~ table schema
        TableSchema tableSchema = TableSchema.builder()
                .field("a", DataTypes.STRING().notNull())
                .field("b", DataTypes.BOOLEAN())
                .field("c", DataTypes.INT().notNull())
                .primaryKey("a", "c")
                .build();

        // ~ table data
        GenericRowData rowData = GenericRowData.of(
                StringData.fromString("Tom"),
                false,
                15
        );

        // connector options
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb");
        options.put(MongoDynamicTableFactory.URI.key(), getTestMongoUri());
        options.put(MongoDynamicTableFactory.DATABASE.key(), DEFAULT_DATABASE_NAME_FOR_IT);
        options.put(MongoDynamicTableFactory.COLLECTION.key(), DEFAULT_COLLECTION_NAME_FOR_IT);
        options.put(MongoDynamicTableFactory.SINK_MAX_RETRIES.key(), "3");
        options.put(MongoDynamicTableFactory.SINK_BUFFER_FLUSH_MAX_ROWS.key(), "10");
        options.put(MongoDynamicTableFactory.SINK_BUFFER_FLUSH_INTERVAL.key(), "10");

        DynamicTableSink dynamicTableSink = createMongoDynamicTableSink(tableSchema, options);
        SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) dynamicTableSink
                .getSinkRuntimeProvider(new MockDynamicTableSinkContext());

        SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();

        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
        MongoRowDataSerializationConverter serConverter = new MongoRowDataSerializationConverter(rowType);
        MongoRowDataSinkFunction sinkFunction1 = new MongoRowDataSinkFunction(
                serConverter,
                new String[]{},
                getTestMongoUri(),
                getDefaultTestDatabaseName(),
                getDefaultTestCollectionName(),
                3,
                3,
                0,
                true
        );

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        rowData.setRowKind(RowKind.UPDATE_AFTER);
        environment.<RowData>fromElements(rowData).addSink(sinkFunction); // new PrintSinkFunction<>()
        environment.execute();

        // ~ response
        BsonDocument actual = getTestMongoClient()
                .getDatabase(getDefaultTestDatabaseName())
                .getCollection(getDefaultTestCollectionName(), BsonDocument.class)
                .find()
                .projection(fields(exclude("_id")))
                .first();

        // ~ expected BsonDocument
        BsonDocument expected = new BsonDocument();
        serConverter.toExternal(rowData, expected);

        assertEquals(expected, actual);
    }

    @Test
    public void testSqlInsert() {
        TableEnvironment stEnv = createStreamTableEnv();

        String table1DDL = createMongoTableDDL(TEST_TABLE_1);
        String table2DDL = createMongoTableDDL(TEST_TABLE_2);

        stEnv.executeSql(table1DDL);
        stEnv.executeSql(table2DDL);

        String insertStatement = "INSERT INTO " + TEST_TABLE_2 + " SELECT * FROM " + TEST_TABLE_1;

        // wait to finish
        TableEnvUtil.execInsertSqlAndWaitResult(stEnv, insertStatement);

        String query = "SELECT " +
                "  h.rowkey, " +
                "  h.family1.col1, " +
                "  h.family2.col1, " +
                "  h.family2.col2, " +
                "  h.family3.col1, " +
                "  h.family3.col2, " +
                "  h.family3.col3, " +
                "  h.family4.col1, " +
                "  h.family4.col2, " +
                "  h.family4.col3, " +
                "  h.family4.col4 " +
                " FROM " + TEST_TABLE_1 + " AS h";
        Iterator<Row> collected = stEnv.executeSql(query).collect();
        List<String> actual = Lists.newArrayList(collected).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("1,10,Hello-1,100,1.01,false,Welt-1,2019-08-18T19:00,2019-08-18,19:00,12345678.0001");
        expected.add("2,20,Hello-2,200,2.02,true,Welt-2,2019-08-18T19:01,2019-08-18,19:01,12345678.0002");
        expected.add("3,30,Hello-3,300,3.03,false,Welt-3,2019-08-18T19:02,2019-08-18,19:02,12345678.0003");
        expected.add("4,40,null,400,4.04,true,Welt-4,2019-08-18T19:03,2019-08-18,19:03,12345678.0004");
        expected.add("5,50,Hello-5,500,5.05,false,Welt-5,2019-08-19T19:10,2019-08-19,19:10,12345678.0005");
        expected.add("6,60,Hello-6,600,6.06,true,Welt-6,2019-08-19T19:20,2019-08-19,19:20,12345678.0006");
        expected.add("7,70,Hello-7,700,7.07,false,Welt-7,2019-08-19T19:30,2019-08-19,19:30,12345678.0007");
        expected.add("8,80,null,800,8.08,true,Welt-8,2019-08-19T19:40,2019-08-19,19:40,12345678.0008");
        assertEquals(expected, actual);
    }

    @Test
    public void testSqlJoin() {

    }

    private String createMongoTableDDL(String tableName) {
        return "CREATE TABLE " + tableName + "(\n" +
            "   rowkey INT," +
            "   family1 ROW<col1 INT>,\n" +
            "   family2 ROW<col1 VARCHAR, col2 BIGINT>,\n" +
            "   family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 VARCHAR>" +
            ") WITH (\n" +
            "   'connector' = 'hbase-1.4',\n" +
            "   'table-name' = '" + tableName + "',\n" +
            "   'zookeeper.znode.parent' = '/hbase' " +
            ")";
    }

    // --------------------------------------------------------
    // DataStream connector integration test
    // --------------------------------------------------------

    /**
     * Sink RowData
     */
//    @Test
//    public void testDataStreamSink() {
//        //RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
//
//        RowType rowType1 = RowType.of(
//                CharType.ofEmptyLiteral()
//        );
//        RowDataMongoConverter rowDataMongoConverter = new RowDataMongoConverter(rowType);
//
//        MongoSinkFunction sinkFunction = new MongoSinkFunction(
//                rowDataMongoConverter,
//                getMongoUriForIT(),
//                getDefaultDatabaseNameForIT(),
//                getDefaultCollectionNameForIT(),
//                3,
//                3
//        );
//
//        StreamExecutionEnvironment stEnv = createStreamEnv();
//        rowData.setRowKind(RowKind.UPDATE_AFTER);
//        stEnv.<RowData>fromElements(rowData).addSink(sinkFunction); // new PrintSinkFunction<>()
//    }

    /**
     * Sink Tuple
     */
    @Test
    public void testDataStreamSinkTuple() {

    }

    // --------------------------------------------------------
    // DataStream test utilities
    // --------------------------------------------------------

    private static StreamExecutionEnvironment createStreamEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }



    /**
     * Mock {@link MongoDynamicTableFactory#createDynamicTableSource(DynamicTableFactory.Context)}
     *
     * @see MongoDynamicTableFactoryContext
     */
    private static DynamicTableSource createMongoDynamicTableSource(TableSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(schema, options, "mock source"),
                new Configuration(),
                MongoDynamicTableFactory.class.getClassLoader());
    }

    /**
     * Mock {@link MongoDynamicTableFactory#createDynamicTableSink(DynamicTableFactory.Context)}
     *
     * @see MongoDynamicTableFactoryContext
     */
    private static DynamicTableSink createMongoDynamicTableSink(TableSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(schema, options, "mock sink"),
                new Configuration(),
                MongoDynamicTableFactory.class.getClassLoader());
    }

    /**
     * Usage:
     *
     * 1. DynamicTableSink dynamicTableSink = new MongoDynamicTableFactory().
     *                 createDynamicTableSink(new MongoDynamicTableFactoryContext());
     *
     * 2. DynamicTableSource dynamicTableSource = new MongoDynamicTableFactory()
     *                 .createDynamicTableSource(new MongoDynamicTableFactoryContext());
     *
     * @see #createMongoDynamicTableSource(TableSchema, Map)
     * @see #createMongoDynamicTableSink(TableSchema, Map)
     */
    private static class MongoDynamicTableFactoryContext implements DynamicTableFactory.Context {
        @Override
        public ObjectIdentifier getObjectIdentifier() {
            return null;
        }

        @Override
        public CatalogTable getCatalogTable() {
            return null;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return null;
        }
    }

    /**
     * Mock {@link DynamicTableSink.Context}
     */
    private static class MockDynamicTableSinkContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType consumedDataType) {
            return null;
        }
    }

    /**
     * Mock {@link DynamicTableSource.Context}
     */
    private static class MockDynamicTableSourceContext implements DynamicTableSource.Context {
        @Override
        public TypeInformation<?> createTypeInformation(DataType producedDataType) {
            return null;
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType producedDataType) {
            return null;
        }
    }

    private static class MockDynamicTableScanText implements ScanTableSource.ScanContext {
        @Override
        public TypeInformation<?> createTypeInformation(DataType producedDataType) {
            return null;
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType producedDataType) {
            return null;
        }
    }

    // --------------------------------------------------------
    // Table test utilities
    // --------------------------------------------------------

    /**
     * Creates a Stream {@link TableEnvironment}
     */
    private static TableEnvironment createStreamTableEnv() {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        return StreamTableEnvironment.create(streamEnv, streamSettings);
    }

    /**
     * Creates a Batch {@link TableEnvironment}
     */
    private TableEnvironment createBatchTableEnv() {
        EnvironmentSettings batchSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        return TableEnvironment.create(batchSettings);
    }
}
