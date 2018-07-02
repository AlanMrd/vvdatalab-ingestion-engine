package br.com.vvdatalab.dataaccess;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;


public class HbaseImpl<T> implements Hbase<T> {

	@Override
	public Connection getConnection() {
		Connection connection = null;
		try {
			Configuration config = HBaseConfiguration.create();			
			connection = ConnectionFactory.createConnection(config);
			System.out.println("Conexao aberta com o HBASE!");
		} catch (IOException e) {
			System.out.println("Error connection with HBase.");
			e.printStackTrace();
		}

		return connection;

	}


	@SuppressWarnings("unchecked")
	@Override
	public T getAllFieldHbase(String hbaseTable, String rowkey, Class<T> classConfig) throws InstantiationException, IllegalAccessException {
		Table table;		
		T clazz = classConfig.newInstance();

		Connection connection = null;
		try {
			connection = getConnection();
			table = connection.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)));

			Get get = new Get(Bytes.toBytes(rowkey));

			Result result = table.get(get);
			NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap("config".getBytes());

			Map<String, String> mapString = new HashMap<String, String>();

			for (Entry<byte[], byte[]> map : familyMap.entrySet()) {
				String campo = new String(map.getKey());
				String valor = new String(map.getValue());
				System.out.println(campo + "-" + valor);

				mapString.put(campo, valor);
			}

			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			clazz = objectMapper.convertValue(mapString, classConfig);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		return clazz;
	}


	@Override
	public void putAll(Dataset<Row> ds) {
		ds.foreachPartition(new ForeachPartitionFunction<Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<Row> t) throws Exception {
				Configuration config = HBaseConfiguration.create();
				Connection connection = ConnectionFactory.createConnection(config);
				Table table = connection.getTable(TableName.valueOf("ingestion:marca".getBytes()));
				System.out.println("Conexao aberta com o HBASE! " + connection.hashCode());
				
				String[] columns = null;

				while (t.hasNext()) {
					Row row = t.next();
					
					Put put = new Put(Bytes.toBytes(row.getAs("rowkey").toString()));
							
					for(String column : columns){
						put.addColumn("dados".getBytes(), column.getBytes(), Bytes.toBytes(row.getAs(column) == null ? "".toString() : row.getAs(column).toString()));
					}
				
					table.put(put);
				}	
			}
		});
		
	}
}
	
