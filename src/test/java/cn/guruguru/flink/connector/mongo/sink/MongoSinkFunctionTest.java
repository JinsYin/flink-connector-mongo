package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
import cn.guruguru.flink.connector.mongo.internal.conveter.RowDataMongoConverter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.Arrays;

public class MongoSinkFunctionTest extends MongoTestingClusterAutoStarter {

    @Test
    public void testSinkRowData() throws Exception {
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("name", new CharType()),
                new RowType.RowField("age", new IntType())
        ));
        RowDataMongoConverter rowDataMongoConverter = new RowDataMongoConverter(rowType);

        // function
        MongoRowDataSinkFunction sinkFunction = new MongoRowDataSinkFunction(
                rowDataMongoConverter,
                getTestMongoUri(),
                getDefaultTestDatabaseName(),
                getDefaultTestCollectionName(),
                3,
                3
        );

        // data
        GenericRowData row1 = GenericRowData.of(StringData.fromString("alice"), 20);
        GenericRowData row2 = GenericRowData.of(StringData.fromString("bob"), 21);
        GenericRowData row3 = GenericRowData.of(StringData.fromString("tom"), 22);
        GenericRowData row4 = GenericRowData.of(StringData.fromString("john"), 23);

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
