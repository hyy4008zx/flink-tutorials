package com.flink.streaming.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

import java.util.concurrent.ConcurrentHashMap;


public class StreamSQLDemo8 {

	public static void main(String[] args) throws Exception {
		final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
		builder.inStreamingMode();


		builder.useBlinkPlanner();


		final EnvironmentSettings settings = builder.build();
		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);


		DataStream<Tuple3<String, Long, Long>> myInts = env.fromElements(
				new Tuple3<>("book", 1L, 150L),
				new Tuple3<>("book", 3L, 110L),
				new Tuple3<>("book", 1L, 120L),
				//new Tuple3<>("book", 3, 10),
				//new Tuple3<>("book", 8, 11)
//				new Tuple3<>("fruit", 4, 33),
//				new Tuple3<>("fruit", 3, 44),
				new Tuple3<>("fruit", 5L, 22L)
				).keyBy(0, 1);
//		"book,4,11,1",
//				"book,1,12,2",
//				"fruit,5,22,1",
//				"fruit,4,33,2")

		Table ds = tEnv.fromDataStream(myInts, "category,shopId,num");


		tEnv.registerTable("T", ds);

		String sql = "SELECT category, shopId, SUM(num) FROM ("+
        "SELECT category, shopId, num, ROW_NUMBER() OVER (PARTITION BY category  ORDER BY num DESC) as rank_num FROM T) "
        + " WHERE rank_num <= 3  group by category, shopId ";

		Table rTable = tEnv.sqlQuery(sql);


		tEnv.registerTableSink("testSink", new MemoryUpsertSink(rTable.getSchema()));

		rTable.insertInto(new StreamQueryConfig(2,300005), "testSink");
		//rTable.insertInto("testSink");
		env.execute();
	}

	public static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple3<String, Long, Long>> {
		private TableSchema schema;
		private String[] keyFields;
		private boolean isAppendOnly;

		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public MemoryUpsertSink(String[] fieldNames) {
			this.fieldNames = fieldNames;
		}

		public MemoryUpsertSink(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
		}

		public MemoryUpsertSink(TableSchema schema) {
			this.schema = schema;
		}

		@Override
		public void setKeyFields(String[] keys) {
			for (String key : keys){
				System.out.println(key + "======");
			}
			this.keyFields = new String[]{this.schema.getFieldNames()[0],this.schema.getFieldNames()[1]};
		}

		@Override
		public void setIsAppendOnly(Boolean isAppendOnly) {
			this.isAppendOnly = isAppendOnly;
			System.out.println("==========####isAppendOnly="+isAppendOnly);
		}

		@Override
		public TypeInformation<Tuple3<String, Long, Long>> getRecordType() {
			return TypeInformation.of(new TypeHint<Tuple3<String, Long, Long>>(){});
		}

		@Override
		public void emitDataStream(DataStream<Tuple2<Boolean, Tuple3<String, Long, Long>>> dataStream) {
			consumeDataStream(dataStream);
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple3<String, Long, Long>>> dataStream) {
			return dataStream.addSink(new DataSink()).setParallelism(1);
		}

		@Override
		public TableSink<Tuple2<Boolean, Tuple3<String, Long, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			MemoryUpsertSink memoryUpsertSink = new MemoryUpsertSink(fieldNames);
			memoryUpsertSink.setFieldNames(fieldNames);
			memoryUpsertSink.setFieldTypes(fieldTypes);

			return memoryUpsertSink;
		}

		@Override
		public String[] getFieldNames() {
			return schema.getFieldNames();
		}

		public void setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
		}

		@Override
		public TypeInformation<?>[] getFieldTypes() {
			return schema.getFieldTypes();
		}

		public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
			this.fieldTypes = fieldTypes;
		}
	}

	private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple3<String, Long, Long>>>{
		private volatile ConcurrentHashMap<String, Tuple2<String, String>> data = new ConcurrentHashMap<>();

		public DataSink() {
		}

		@Override
		public void invoke(Tuple2<Boolean, Tuple3<String, Long, Long>> value, Context context) throws Exception {

			System.out.println("===" + value);

		}
	}
}
