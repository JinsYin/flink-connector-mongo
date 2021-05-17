package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataSerializationConverter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MongoSinkFunctionTest extends MongoTestingClusterAutoStarter {

    /**
     * @see cn.guruguru.flink.connector.mongo.internal.converter.MongoRowDataSerConverterTest
     */
    @Test
    public void testSinkRowData() throws Exception {
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("name", new CharType()),
                new RowType.RowField("age", new IntType()),
                new RowType.RowField("scores", new ArrayType(new DoubleType())),
                new RowType.RowField("map", new MapType(new CharType(), new IntType())),
                new RowType.RowField("ts", new TimestampType()), // ZonedTimestampType, LocalZonedTimestampType
                new RowType.RowField("info", new RowType(Arrays.asList(
                        new RowType.RowField("col1", new CharType()),
                        new RowType.RowField("col2", new ArrayType(new DoubleType())),
                        new RowType.RowField("col3", new RowType(
                                Collections.singletonList(new RowType.RowField("colcol", new CharType()))))
                )))
        ));
        MongoRowDataSerializationConverter serConverter = new MongoRowDataSerializationConverter(rowType);

        // function
        MongoRowDataSinkFunction sinkFunction = new MongoRowDataSinkFunction(
                serConverter,
                getTestMongoUri(),
                getDefaultTestDatabaseName(),
                getDefaultTestCollectionName(),
                3,
                3,
                3,
                true
        );

        // data
        Map<StringData, Integer> genericMap = new HashMap<>();
        genericMap.put(StringData.fromString("k1"), 1);
        genericMap.put(StringData.fromString("k2"), 2);
        GenericRowData row1 = GenericRowData.of(
                StringData.fromString("alice"),
                20,
                new GenericArrayData(new double[]{95.1, 95.2}),
                new GenericMapData(genericMap),
                TimestampData.fromLocalDateTime(LocalDateTime.now()),
                GenericRowData.of(
                    StringData.fromString("k1"),
                    new GenericArrayData(new double[]{95.1, 95.2}),
                    GenericRowData.of(StringData.fromString("cc1")
                )));
        GenericRowData row2 = GenericRowData.of(
                StringData.fromString("bob"),
                21,
                new GenericArrayData(new double[]{96.1, 96.2}),
                new GenericMapData(genericMap),
                TimestampData.fromLocalDateTime(LocalDateTime.now()),
                GenericRowData.of(
                    StringData.fromString("k2"),
                    new GenericArrayData(new double[]{96.1, 96.2}),
                    GenericRowData.of(StringData.fromString("cc2"))
                ));
        GenericRowData row3 = GenericRowData.of(
                StringData.fromString("tom"),
                22,
                new GenericArrayData(new double[]{97.1, 97.2}),
                new GenericMapData(genericMap),
                TimestampData.fromLocalDateTime(LocalDateTime.now()),
                GenericRowData.of(
                    StringData.fromString("k3"),
                    new GenericArrayData(new double[]{97.1, 97.2}),
                    GenericRowData.of(StringData.fromString("cc3"))
                ));
        GenericRowData row4 = GenericRowData.of(
                StringData.fromString("john"),
                23,
                new GenericArrayData(new double[]{98.1, 98.2}),
                new GenericMapData(genericMap),
                TimestampData.fromLocalDateTime(LocalDateTime.now()),
                GenericRowData.of(
                    StringData.fromString("k4"),
                    new GenericArrayData(new double[]{98.1, 98.2}),
                    GenericRowData.of(StringData.fromString("cc4"))
                ));

        row1.setRowKind(RowKind.INSERT);
        row2.setRowKind(RowKind.DELETE);
        row3.setRowKind(RowKind.UPDATE_BEFORE);
        row4.setRowKind(RowKind.UPDATE_AFTER);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> dataStreamSource = env.fromElements(row1, row2, row3, row4);
        dataStreamSource.addSink(sinkFunction); // new PrintSinkFunction<>()

        env.execute();
    }

    /**
     * TODO
     */
    @Test
    public void testSinkPojo() {

    }

    /**
     * TODO
     */
    @Test
    public void testSinkTuple() {

    }

}
