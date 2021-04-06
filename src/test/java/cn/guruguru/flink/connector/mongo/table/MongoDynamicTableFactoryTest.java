package cn.guruguru.flink.connector.mongo.table;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link MongoDynamicTableFactory}
 *
 * see org.apache.flink.table.factories.PrintSinkFactoryTest
 */
public class MongoDynamicTableFactoryTest {

    private static final String FAMILY1 = "f1";
    private static final String FAMILY2 = "f2";
    private static final String FAMILY3 = "f3";
    private static final String FAMILY4 = "f4";
    private static final String COL1 = "c1";
    private static final String COL2 = "c2";
    private static final String COL3 = "c3";
    private static final String COL4 = "c4";
    private static final String ObjectId = "_id";

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void testDynamicTableSourceFactory() {

    }

    @Test
    public void testDynamicTableSinkFactory() {
        TableSchema schema = TableSchema.builder()
                .field(ObjectId, STRING())
                .field(FAMILY1, ROW(
                        FIELD(COL1, DOUBLE()),
                        FIELD(COL2, INT())))
                .field(FAMILY2, ROW(
                        FIELD(COL1, INT()),
                        FIELD(COL3, BIGINT())))
                .field(FAMILY3, ROW(
                        FIELD(COL2, BOOLEAN()),
                        FIELD(COL3, STRING())))
                .field(FAMILY4, ROW(
                        FIELD(COL1, DECIMAL(10, 3)),
                        FIELD(COL2, TIMESTAMP(3)),
                        FIELD(COL3, DATE()),
                        FIELD(COL4, TIME())))
                .build();

        DynamicTableSink sink = createDynamicTableSink(schema, getAllOptions());
        assertTrue(sink instanceof MongoDynamicTableSink);
        MongoDynamicTableSink mongoSink = (MongoDynamicTableSink) sink;
    }

    // -----

    @Test
    public void validateEmptyConfiguration() {
        thrown.expect(ValidationException.class);
        thrown.expectMessage(
                "One or more required options are missing.\n" +
                        "\n" +
                        "Missing required options are:\n" +
                        "\n" +
                        "uri\n" +
                        "database\n" +
                        "collection");

        TableSchema tableSchema = TableSchema.builder()
                .field("a", DataTypes.TIME())
                .build();
        Map<String, String> emptyOption = new HashMap<>();
        createDynamicTableSink(tableSchema, emptyOption);
    }

    // -----

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "mongodb");
        options.put("database", "testMongoDatabase");
        options.put("collection", "testMongoCollection");
        options.put("uri", "mongo://127.0.0.1:27017");
        return options;
    }

    /**
     * {@link MongoDynamicTableFactory#createDynamicTableSource(DynamicTableFactory.Context)}
     */
    private static DynamicTableSource createDynamicTableSource(TableSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(schema, options, "mock source"),
                new Configuration(),
                MongoDynamicTableFactory.class.getClassLoader());
    }

    /**
     * {@link MongoDynamicTableFactory#createDynamicTableSink(DynamicTableFactory.Context)}
     */
    private static DynamicTableSink createDynamicTableSink(TableSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSink(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new CatalogTableImpl(schema, options, "mock sink"),
                new Configuration(),
                MongoDynamicTableFactory.class.getClassLoader());
    }
}
