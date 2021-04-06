package cn.guruguru.flink.connector.mongo.internal.converter;

import cn.guruguru.flink.connector.mongo.internal.conveter.RowDataMongoConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.RowType;
import org.bson.*;
import org.bson.types.Decimal128;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit Test for {@link RowDataMongoConverter}
 */
public class RowDataMongoConverterTest {

    /**
     * @see RowDataMongoConverter#toExternal(RowData, BsonDocument)
     */
    @Test
    public void testToExternal() {
        // ~ table schema

        TableSchema tableSchema = TableSchema.builder()
                .field("a", DataTypes.NULL())
                .field("b", DataTypes.BOOLEAN())
                .field("c", DataTypes.DOUBLE().notNull())
                .field("d", DataTypes.BINARY(3))    // length: 3
                .field("e", DataTypes.VARBINARY(2)) // length: 2
                .field("f", DataTypes.TINYINT().notNull())
                .field("g", DataTypes.BIGINT())
                .field("h", DataTypes.DATE())
                .field("i", DataTypes.TIME())
                .field("j", DataTypes.TIMESTAMP())
                .field("k", DataTypes.STRING().notNull())
                .field("l", DataTypes.ROW(
                    DataTypes.FIELD("a1", DataTypes.INT()),
                    DataTypes.FIELD("a2", DataTypes.BOOLEAN()),
                    DataTypes.FIELD("a3", DataTypes.STRING())
                ))
                .field("m", DataTypes.MAP(
                    DataTypes.STRING(),
                    DataTypes.INT()
                ))
                .field("n", DataTypes.MAP(
                    DataTypes.STRING(),
                    DataTypes.ROW(
                        DataTypes.FIELD("r1", DataTypes.STRING()),
                        DataTypes.FIELD("r2", DataTypes.BOOLEAN())
                    )
                ))
                .field("o", DataTypes.ARRAY(
                    DataTypes.INT()
                ))
                .field("p", DataTypes.ARRAY(
                    DataTypes.ROW(
                        DataTypes.FIELD("x1", DataTypes.INT()),
                        DataTypes.FIELD("y1", DataTypes.BOOLEAN())
                    )
                ))
                .field("q", DataTypes.DECIMAL(20, 3)) // precision = 10, scale = 3
                .primaryKey("k", "f", "c")
                .build();
        RowType rowType = tableSchemaToRowType(tableSchema);

        // ~ table data

        Map<StringData, Integer> map1 = new HashMap<>();
        map1.put(StringData.fromString("k1"), 21);
        map1.put(StringData.fromString("k2"), 22);

        Map<StringData, RowData> map2 = new HashMap<>();
        map2.put(StringData.fromString("key1"), GenericRowData.of(StringData.fromString("value1"), true));
        map2.put(StringData.fromString("key2"), GenericRowData.of(StringData.fromString("value2"), false));

        GenericArrayData arrayData1 = new GenericArrayData(new Integer[]{1, 5, 7});

        Object[] rowObjects = new Object[]{
            GenericRowData.of(11, true),
            GenericRowData.of(12, false)
        };
        GenericArrayData arrayData2 = new GenericArrayData(rowObjects);

        GenericRowData rowData = GenericRowData.of(
                null,
                true,
                19.19,
                new byte[]{1, 3, 5, 7, 9},
                new byte[]{2, 4, 6, 8, 10},
                (byte) 127,
                123456789L,   // Long
                2,            // 2d, DATE <--> int
                1000,         // 1s, TIME <--> int
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12")),
                StringData.fromString("NBA"),
                GenericRowData.of(29, false, StringData.fromString("bob")),
                new GenericMapData(map1),
                new GenericMapData(map2),
                arrayData1,
                arrayData2,
                new BigDecimal("12345678.1567")
        );

        // ~ converted mongodb data

        RowDataMongoConverter converter = new RowDataMongoConverter(rowType);
        BsonDocument actual = converter.toExternal(rowData, new BsonDocument());

        // ~ expected mongodb data

        BsonDocument expected = new BsonDocument();
        expected.append("a", new BsonNull());
        expected.append("b", new BsonBoolean(true));
        expected.append("c", new BsonDouble( 19.19));
        expected.append("d", new BsonBinary(new byte[]{1, 3, 5, 7, 9}));
        expected.append("e", new BsonBinary(new byte[]{2, 4, 6, 8, 10}));
        expected.append("f", new BsonBinary(new byte[]{(byte) 127}));
        expected.append("g", new BsonInt64(123456789L));
        expected.append("h", new BsonDateTime(TimeUnit.DAYS.toMillis(2))); // 2d
        expected.append("i", new BsonDateTime(1000)); // 1s
        expected.append("j", new BsonTimestamp(TimestampData
                .fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12")).getMillisecond()));
        expected.append("k", new BsonString("NBA"));
        expected.append("l", new BsonDocument(Arrays.asList(
                new BsonElement("a1", new BsonInt32(29)),
                new BsonElement("a2", new BsonBoolean(false)),
                new BsonElement("a3", new BsonString("bob"))
        )));
        expected.append("m", new BsonDocument(Arrays.asList(
                new BsonElement("k1", new BsonInt32(21)),
                new BsonElement("k2", new BsonInt32(22))
        )));
        expected.append("n", new BsonDocument(Arrays.asList(
                new BsonElement("key1", new BsonDocument(Arrays.asList(
                        new BsonElement("r1", new BsonString("value1")),
                        new BsonElement("r2", new BsonBoolean(true))
                ))),
                new BsonElement("key2", new BsonDocument(Arrays.asList(
                        new BsonElement("r1", new BsonString("value2")),
                        new BsonElement("r2", new BsonBoolean(false))
                )))
        )));
        expected.append("o", new BsonArray(Arrays.asList(
                new BsonInt32(1),
                new BsonInt32(5),
                new BsonInt32(7)
        )));
        expected.append("p", new BsonArray(Arrays.asList(
                new BsonDocument(Arrays.asList(
                        new BsonElement("x1", new BsonInt32(11)),
                        new BsonElement("y1", new BsonBoolean(true))
                )),
                new BsonDocument(Arrays.asList(
                        new BsonElement("x1", new BsonInt32(12)),
                        new BsonElement("y1", new BsonBoolean(false))
                ))
        )));
        expected.append("q", new BsonDecimal128(new Decimal128(new BigDecimal("12345678.157"))));

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testToInternal() {
        BsonDocument expected = new BsonDocument();
        expected.append("a", new BsonDouble(1.2f));
    }

    /**
     * TODO
     */
    @Test
    public void testUnsupportedType() {}

    // ~ utilities ------------------------------------------------

    private RowType tableSchemaToRowType(TableSchema tableSchema) {
        return (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();
    }


}
