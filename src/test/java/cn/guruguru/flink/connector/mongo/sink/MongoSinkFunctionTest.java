package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataSerializationConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.bson.BsonDocument;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.fields;
import static org.junit.Assert.assertEquals;

public class MongoSinkFunctionTest extends MongoTestingClusterAutoStarter {

    StreamExecutionEnvironment streamEnv;

    @Before
    public void setupEnvironment() {
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * @see cn.guruguru.flink.connector.mongo.internal.converter.MongoRowDataSerConverterTest
     */
    @Test
    public void testSinkRowData() throws Exception {
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("name", new CharType()),
                new RowType.RowField("age", new IntType()),
                new RowType.RowField("score", new DoubleType()),
                new RowType.RowField("data", new RowType(Arrays.asList(
                        new RowType.RowField("code", new IntType()),
                        new RowType.RowField("msg", new CharType())
                )))
        ));
        MongoRowDataSerializationConverter serConverter = new MongoRowDataSerializationConverter(rowType);

        // data for appending
        GenericRowData appendedRow = GenericRowData.of(
                StringData.fromString("alice"),
                20,
                90.5,
                GenericRowData.of(10001, StringData.fromString("error")));
        appendedRow.setRowKind(RowKind.INSERT);
        BsonDocument appendedDoc = serConverter.toExternal(appendedRow);

        // data for inserting
        GenericRowData insertedRow = GenericRowData.of(
                StringData.fromString("john"),
                30,
                91.5,
                GenericRowData.of(10002, StringData.fromString("warn")));
        BsonDocument insertedDoc = serConverter.toExternal(insertedRow);

        // data for replacing
        GenericRowData replacedRow = GenericRowData.of(
                StringData.fromString("alice"),
                20,
                90.5,
                GenericRowData.of(10003, StringData.fromString("info")));
        BsonDocument replacedDoc = serConverter.toExternal(replacedRow);

        // append
        MongoRowDataSinkFunction appendSinkFunction = createSinkFunction(serConverter);
        executeSink(appendSinkFunction, appendedRow);
        List<BsonDocument> appendedResult = findAllDocuments();
        assertEquals(appendedResult.size(), 1);
        assertEquals(appendedDoc, appendedResult.get(0));

        // upsert (insert)
        MongoRowDataSinkFunction upsertSinkFunction = createSinkFunction(serConverter, "name");
        executeSink(upsertSinkFunction, insertedRow);
        List<BsonDocument> insertedResult = findAllDocuments();
        assertEquals(2, insertedResult.size());
        assertEquals(insertedResult, Arrays.asList(appendedDoc, insertedDoc));

        // upsert (replace) 主键之间调换顺序并间隔一位
        MongoRowDataSinkFunction upsertSinkFunction2 = createSinkFunction(serConverter, "score", "name");
        executeSink(upsertSinkFunction2, replacedRow);
        List<BsonDocument> replacedResult = findAllDocuments();
        assertEquals(replacedResult.size(), 2);
        assertEquals(replacedResult, Arrays.asList(replacedDoc, insertedDoc));
    }

    private void executeSink(MongoRowDataSinkFunction sinkFunction, RowData rowData) throws Exception {
        streamEnv.<RowData>fromElements(rowData).addSink(sinkFunction); // new PrintSinkFunction<>()
        streamEnv.execute();
    }

    // ----------------------------------------------------------------------

    public static MongoRowDataSinkFunction createSinkFunction(
            MongoRowDataSerializationConverter serConverter, String... keyNames) {
        return new MongoRowDataSinkFunction(
                serConverter,
                keyNames,
                getTestMongoUri(),
                getDefaultTestDatabaseName(),
                getDefaultTestCollectionName(),
                3,
                3,
                0,
                true
        );
    }

    public static List<BsonDocument> findAllDocuments() {
        List<BsonDocument> documents = new ArrayList<>();
        getTestMongoClient()
                .getDatabase(getDefaultTestDatabaseName())
                .getCollection(getDefaultTestCollectionName(), BsonDocument.class)
                .find()
                .projection(fields(exclude("_id")))
                .forEach(documents::add);
        return documents;
    }

}
