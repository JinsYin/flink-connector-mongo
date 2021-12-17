package cn.guruguru.flink.connector.mongo.source;

import cn.guruguru.flink.connector.mongo.MongoTestingClusterAutoStarter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/**
 * Unit test for {@link MongoRowDataLookupFunction}
 */
public class MongoRowDataLookupFunctionTest extends MongoTestingClusterAutoStarter  {

    private static final String LOOKUP_TABLE = "lookup";

    /**
     * Coped from flin-connector-jdbc's `org.apache.flink.connector.jdbc.table.JdbcLookupTableITCase`
     */
    @Test
    public void testLookupArray() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Iterator<Row> collected = useDynamicTableFactory(env, tEnv);
        List<String> result = Lists.newArrayList(collected).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("1,1,11-c1-v1,11-c2-v1");
        expected.add("1,1,11-c1-v1,11-c2-v1");
        expected.add("1,1,11-c1-v2,11-c2-v2");
        expected.add("1,1,11-c1-v2,11-c2-v2");
        expected.add("2,3,null,23-c2");
        expected.add("2,5,25-c1,25-c2");
        expected.add("3,8,38-c1,38-c2");
        Collections.sort(expected);

        assertEquals(expected, result);
    }

    private Iterator<Row> useDynamicTableFactory(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
        boolean useCache = false;

        Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
                new Tuple2<>(1, "1"),
                new Tuple2<>(1, "1"),
                new Tuple2<>(2, "3"),
                new Tuple2<>(2, "5"),
                new Tuple2<>(3, "5"),
                new Tuple2<>(3, "8")
        )), $("id1"), $("id2"), $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        String cacheConfig = ", 'lookup.cache.max-rows'='4', 'lookup.cache.ttl'='10000', 'lookup.max-retries'='5'";
        tEnv.executeSql(
                String.format("create table lookup (" +
                        "  id1 INT," +
                        "  id2 VARCHAR," +
                        "  comment1 VARCHAR," +
                        "  comment2 VARCHAR" +
                        ") with(" +
                        "  'connector'='mongodb'," +
                        "  'url'='" + getTestMongoUri() + "'," +
                        "  'table-name'='" + LOOKUP_TABLE + "'" +
                        "  %s)", useCache ? cacheConfig : ""));

        String sqlQuery = "SELECT source.id1, source.id2, L.comment1, L.comment2 FROM T AS source " +
                "JOIN lookup for system_time as of source.proctime AS L " +
                "ON source.id1 = L.id1 and source.id2 = L.id2";
        return tEnv.executeSql(sqlQuery).collect();
    }
}
