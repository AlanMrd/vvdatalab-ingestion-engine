package br.com.vvdatalab.dataaccess;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import br.com.vvdatalab.dto.HbaseConfig;

public interface HBaseDAO extends Serializable{
	public Connection getConnection();	
	<T> T getAllFieldHbase(String hbaseTable, String rowkey, Class<T> classConfig) throws InstantiationException, IllegalAccessException;
	void putAll(Dataset<Row> ds, String[] columns, HbaseConfig hbaseConfig) throws IOException;
	<T> List<T> scanRow(byte[] prefixRowStart, byte[] prefixRowEnd, Class<T> clazz) throws InstantiationException, IllegalAccessException;
}
