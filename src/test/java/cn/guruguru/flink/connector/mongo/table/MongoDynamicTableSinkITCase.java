package cn.guruguru.flink.connector.mongo.table;

public class MongoDynamicTableSinkITCase {

//    @Test
//    public void testUpsert() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().enableObjectReuse();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        Table t = tEnv.fromDataStream(get4TupleDataStream(env).assignTimestampsAndWatermarks(
//                new AscendingTimestampExtractor<Tuple4<Integer, Long, String, Timestamp>>() {
//                    @Override
//                    public long extractAscendingTimestamp(Tuple4<Integer, Long, String, Timestamp> element) {
//                        return element.f0;
//                    }
//                }), $("id"), $("num"), $("text"), $("ts"));
//
//        tEnv.createTemporaryView("T", t);
//        tEnv.executeSql(
//                "CREATE TABLE upsertSink (" +
//                        "  cnt BIGINT," +
//                        "  lencnt BIGINT," +
//                        "  cTag INT," +
//                        "  ts TIMESTAMP(3)" +
//                        ") WITH (" +
//                        "  'connector.type'='jdbc'," +
//                        "  'connector.url'='" + DB_URL + "'," +
//                        "  'connector.table'='" + OUTPUT_TABLE1 + "'" +
//                        ")");
//
//        TableResult tableResult = tEnv.executeSql("INSERT INTO upsertSink \n" +
//                "SELECT cnt, COUNT(len) AS lencnt, cTag, MAX(ts) AS ts\n" +
//                "FROM (\n" +
//                "  SELECT len, COUNT(id) as cnt, cTag, MAX(ts) AS ts\n" +
//                "  FROM (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag, ts FROM T)\n" +
//                "  GROUP BY len, cTag\n" +
//                ")\n" +
//                "GROUP BY cnt, cTag");
//        // wait to finish
//        tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
//        check(new Row[] {
//                Row.of(1, 5, 1, Timestamp.valueOf("1970-01-01 00:00:00.006")),
//                Row.of(7, 1, 1, Timestamp.valueOf("1970-01-01 00:00:00.021")),
//                Row.of(9, 1, 1, Timestamp.valueOf("1970-01-01 00:00:00.015"))
//        }, DB_URL, OUTPUT_TABLE1, new String[]{"cnt", "lencnt", "cTag", "ts"});
//    }
//
//    @Test
//    public void testAppend() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().enableObjectReuse();
//        env.getConfig().setParallelism(1);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));
//
//        tEnv.registerTable("T", t);
//
//        tEnv.executeSql(
//                "CREATE TABLE upsertSink (" +
//                        "  id INT," +
//                        "  num BIGINT," +
//                        "  ts TIMESTAMP(3)" +
//                        ") WITH (" +
//                        "  'connector.type'='jdbc'," +
//                        "  'connector.url'='" + DB_URL + "'," +
//                        "  'connector.table'='" + OUTPUT_TABLE2 + "'" +
//                        ")");
//
//        TableResult tableResult = tEnv.executeSql(
//                "INSERT INTO upsertSink SELECT id, num, ts FROM T WHERE id IN (2, 10, 20)");
//        // wait to finish
//        tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
//        check(new Row[] {
//                Row.of(2, 2, Timestamp.valueOf("1970-01-01 00:00:00.002")),
//                Row.of(10, 4, Timestamp.valueOf("1970-01-01 00:00:00.01")),
//                Row.of(20, 6, Timestamp.valueOf("1970-01-01 00:00:00.02"))
//        }, DB_URL, OUTPUT_TABLE2, new String[]{"id", "num", "ts"});
//    }
}
