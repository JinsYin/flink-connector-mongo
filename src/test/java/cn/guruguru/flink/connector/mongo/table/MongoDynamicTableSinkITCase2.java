//package cn.guruguru.flink.connector.mongo.table;
//
//import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
//import cn.guruguru.flink.connector.mongo.internal.conveter.RowDataMongoConverter;
//import cn.guruguru.flink.connector.mongo.sink.MongoSinkFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.TableSchema;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.connector.sink.DynamicTableSink;
//import org.apache.flink.table.connector.sink.SinkFunctionProvider;
//import org.apache.flink.table.data.GenericRowData;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.data.StringData;
//import org.apache.flink.table.factories.FactoryUtil;
//import org.apache.flink.table.types.DataType;
//import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.types.RowKind;
//import org.bson.BsonDocument;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static cn.guruguru.flink.connector.mongo.table.TestContext.context;
//import static com.mongodb.client.model.Projections.exclude;
//import static com.mongodb.client.model.Projections.fields;
//import static org.junit.Assert.assertEquals;
//
//public class MongoDynamicTableSinkITCase extends MongoTestingClusterAutoStarter {
//
//    @Test
//    public void testStreamingSink() throws Exception {
//        TableSchema schema = TableSchema.builder()
//                .field("a", DataTypes.BIGINT().notNull())
////                .field("b", DataTypes.TIME())
//                .field("c", DataTypes.STRING().notNull())
//                .field("d", DataTypes.FLOAT())
////                .field("e", DataTypes.TINYINT().notNull())
////                .field("f", DataTypes.DATE())
////                .field("g", DataTypes.TIMESTAMP().notNull())
//                .primaryKey("a", "c")
//                .build();
//
//        GenericRowData rowData = GenericRowData.of(
//                1L,
////                12345,
//                StringData.fromString("ABCDE"),
//                12.12f//,
////                (byte) 2,
////                12345//,
////                TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12"))
//        );
//
//        String database = "MgDatabase";
//        String collection = "MgCollection";
//
//        Map<String, String> options = new HashMap<>();
//        options.put(FactoryUtil.CONNECTOR.key(), "mongodb");
//        options.put(MongoDynamicTableFactory.MONGO_URI.key(), getMongoUriForIT());
//        options.put(MongoDynamicTableFactory.MONGO_DATABASE.key(), getDefaultDatabaseNameForIT());
//        options.put(MongoDynamicTableFactory.MONGO_COLLECTION.key(), getDefaultCollectionNameForIT());
//        options.put(MongoDynamicTableFactory.LOOKUP_CACHE_MAX_ROWS.key(), "3");
//        options.put(MongoDynamicTableFactory.LOOKUP_CACHE_TTL.key(), "3");
//        options.put(MongoDynamicTableFactory.LOOKUP_MAX_RETRIES.key(), "3");
//        options.put(MongoDynamicTableFactory.SINK_MAX_RETRIES.key(), "3");
//        options.put(MongoDynamicTableFactory.SINK_BUFFER_FLUSH_MAX_ROWS.key(), "10");
//        options.put(MongoDynamicTableFactory.SINK_BUFFER_FLUSH_INTERVAL.key(), "10");
//
//        MongoDynamicTableFactory sinkFactory = new MongoDynamicTableFactory();
//        SinkFunctionProvider sinkRuntimeProvider = (SinkFunctionProvider) sinkFactory.createDynamicTableSink(
//                context()
//                    .withSchema(schema)
//                    .withOption("connector", "mongodb")
//                    .withOption(MongoDynamicTableFactory.MONGO_URI.key(), getMongoUriForIT())
//                    .withOption(MongoDynamicTableFactory.MONGO_DATABASE.key(), getDefaultDatabaseNameForIT())
//                    .withOption(MongoDynamicTableFactory.MONGO_COLLECTION.key(), getDefaultCollectionNameForIT())
//                    .withOption(MongoDynamicTableFactory.LOOKUP_CACHE_MAX_ROWS.key(), "3")
//                    .withOption(MongoDynamicTableFactory.LOOKUP_CACHE_TTL.key(), "3")
//                    .withOption(MongoDynamicTableFactory.LOOKUP_MAX_RETRIES.key(), "3")
//                    .withOption(MongoDynamicTableFactory.SINK_MAX_RETRIES.key(), "3")
//                    .withOption(MongoDynamicTableFactory.SINK_BUFFER_FLUSH_MAX_ROWS.key(), "10")
//                    .withOption(MongoDynamicTableFactory.SINK_BUFFER_FLUSH_INTERVAL.key(), "10")
//                    .build()
//        ).getSinkRuntimeProvider(new MockSinkContext());
//
//        SinkFunctionProvider sinkRuntimeProvider1 = (SinkFunctionProvider) createDynamicTableSink(schema, options)
//                .getSinkRuntimeProvider(new MockSinkContext());
//
//        SinkFunction<RowData> sinkFunction = sinkRuntimeProvider1.createSinkFunction();
//
//        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
//        RowDataMongoConverter rowDataMongoConverter = new RowDataMongoConverter(rowType);
//        MongoSinkFunction sinkFunction1 = new MongoSinkFunction(
//                rowDataMongoConverter,
//                getMongoUriForIT(),
//                getDefaultDatabaseNameForIT(),
//                getDefaultCollectionNameForIT(),
//                3,
//                3
//        );
//
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        rowData.setRowKind(RowKind.UPDATE_AFTER);
//        environment.<RowData>fromElements(rowData).addSink(sinkFunction); // new PrintSinkFunction<>()
//        environment.execute();
//
//        BsonDocument actual = getMongoClientForIT()
//                .getDatabase(getDefaultDatabaseNameForIT())
//                .getCollection(getDefaultCollectionNameForIT(), BsonDocument.class)
//                .find()
//                .projection(fields(exclude("_id")))
//                .first();
//
//        BsonDocument expected = new BsonDocument();
//        rowDataMongoConverter.toExternal(rowData, expected);
//
//        assertEquals(expected, actual);
//    }
//
//    private static class MockSinkContext implements DynamicTableSink.Context {
//        @Override
//        public boolean isBounded() {
//            return false;
//        }
//
//        @Override
//        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
//            return null;
//        }
//
//        @Override
//        public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType consumedDataType) {
//            return null;
//        }
//    }
//
//
//
//    private static StreamTableEnvironment createStreamTableEnv() {
//        return null;
//    }
//
//}
