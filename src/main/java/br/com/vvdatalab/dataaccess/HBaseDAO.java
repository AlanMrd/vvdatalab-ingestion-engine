package br.com.vvdatalab.dataaccess;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface HBaseDAO {
	public Connection getConnection();	
	<T> T getAllFieldHbase(String hbaseTable, String rowkey, Class<T> classConfig) throws InstantiationException, IllegalAccessException;
	void putAll(Dataset<Row> ds, String[] columns);
}
