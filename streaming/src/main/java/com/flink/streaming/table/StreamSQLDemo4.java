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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

import java.util.concurrent.ConcurrentHashMap;


public class StreamSQLDemo4 {

	public static void main(String[] args) throws Exception {
		final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
		builder.inStreamingMode();


		builder.useBlinkPlanner();


		final EnvironmentSettings settings = builder.build();
		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);


		DataStream<Tuple3<String, Integer, Integer>> myInts = env.fromElements(
				new Tuple3<>("book", 1, 150),
				new Tuple3<>("book", 3, 110),
				new Tuple3<>("book", 1, 120),
				//new Tuple3<>("book", 4, 200),
				//new Tuple3<>("book", 1, 200),
				//new Tuple3<>("book", 2, 300),
				//new Tuple3<>("book", 2, 400),
				//new Tuple3<>("book", 4, 500),
				//new Tuple3<>("book", 1, 400),
				new Tuple3<>("fruit", 5, 100)
				);


		Table ds = tEnv.fromDataStream(myInts, "category,shopId,sellId");

		tEnv.registerTable("T", ds);

		String sql = "SELECT category, shopId, avgSellId, rank_num FROM ("+
		"SELECT category, shopId, avgSellId, ROW_NUMBER() OVER (PARTITION BY category ORDER BY avgSellId DESC) as rank_num"+
		" FROM ("+
				"SELECT category, shopId, AVG(sellId) as avgSellId FROM T " +
				"GROUP BY category, shopId))"+
				" WHERE rank_num <= 3";

		Table rTable = tEnv.sqlQuery(sql);

		//tEnv.toRetractStream(rTable, TypeInformation.of(new TypeHint<Tuple4<String, Integer, Integer,Long>>(){})).print();

		tEnv.registerTableSink("testSink", new MemoryUpsertSink(rTable.getSchema()));

		//rTable.insertInto(new StreamQueryConfig(2,300005), "testSink");
		rTable.insertInto("testSink");
		env.execute();
	}

	public static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple4<String, Integer, Integer,Long>> {
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
		public TypeInformation<Tuple4<String, Integer, Integer,Long>> getRecordType() {
			return TypeInformation.of(new TypeHint<Tuple4<String, Integer, Integer,Long>>(){});
		}

		@Override
		public void emitDataStream(DataStream<Tuple2<Boolean, Tuple4<String, Integer, Integer,Long>>> dataStream) {
			consumeDataStream(dataStream);
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple4<String, Integer, Integer,Long>>> dataStream) {
			return dataStream.addSink(new DataSink()).setParallelism(1);
		}

		@Override
		public TableSink<Tuple2<Boolean, Tuple4<String, Integer, Integer,Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
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

	private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple4<String, Integer, Integer,Long>>>{
		private volatile ConcurrentHashMap<String, Tuple2<String, String>> data = new ConcurrentHashMap<>();

		public DataSink() {
		}

		@Override
		public void invoke(Tuple2<Boolean, Tuple4<String, Integer, Integer,Long>> value, Context context) throws Exception {

			System.out.println("===" + value);

		}
	}
}
