package br.com.vvdatalab.dataaccess;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Hbase<T> {
	public Connection getConnection();	
	T getAllFieldHbase(String hbaseTable, String rowkey, Class<T> classConfig) throws InstantiationException, IllegalAccessException;
	void putAll(Dataset<Row> ds);
}
