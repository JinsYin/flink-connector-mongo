package cn.guruguru.flink.connector.mongo.sink;

import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
import cn.guruguru.flink.connector.mongo.internal.conveter.MongoRowDataSerializationConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.CharType;
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
                new RowType.RowField("data", new RowType(Arrays.asList(
                        new RowType.RowField("code", new IntType()),
                        new RowType.RowField("msg", new CharType())
                )
        ))));
        MongoRowDataSerializationConverter serConverter = new MongoRowDataSerializationConverter(rowType);

        // data for appending
        GenericRowData originRow = GenericRowData.of(
                StringData.fromString("alice"),
                20,
                GenericRowData.of(10001, StringData.fromString("error")));
        originRow.setRowKind(RowKind.INSERT);
        BsonDocument originDoc = serConverter.toExternal(originRow);

        // data for non-replacement
        GenericRowData nonReplacementRow = GenericRowData.of(
                StringData.fromString("john"),
                30,
                GenericRowData.of(10002, StringData.fromString("warn")));

        // data for replacing
        GenericRowData replacementRow = GenericRowData.of(
                StringData.fromString("alice"),
                40,
                GenericRowData.of(10003, StringData.fromString("info")));
        BsonDocument replacementDoc = serConverter.toExternal(replacementRow);

        // append
        MongoRowDataSinkFunction appendSinkFunction = createSinkFunction(serConverter);
        executeSink(appendSinkFunction, originRow);
        List<BsonDocument> appendResult = findAllDocuments();
        assertEquals(appendResult.size(), 1);
        assertEquals(originDoc, appendResult.get(0));

        // no replacement
        MongoRowDataSinkFunction upsertSinkFunction = createSinkFunction(serConverter, "name");
        executeSink(upsertSinkFunction, nonReplacementRow);
        List<BsonDocument> nonReplacementResult = findAllDocuments();
        assertEquals(nonReplacementResult.size(), 1);
        assertEquals(originDoc, nonReplacementResult.get(0)); // No change

        // replacement
        MongoRowDataSinkFunction upsertSinkFunction2 = createSinkFunction(serConverter, "name");
        executeSink(upsertSinkFunction2, replacementRow);
        List<BsonDocument> replacementResult = findAllDocuments();
        assertEquals(replacementResult.size(), 1);
        assertEquals(replacementDoc, replacementResult.get(0));
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
