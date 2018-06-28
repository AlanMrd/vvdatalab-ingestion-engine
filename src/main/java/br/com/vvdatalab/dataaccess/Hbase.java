package br.com.vvdatalab.dataaccess;

import org.apache.hadoop.hbase.client.Connection;

public interface Hbase<T> {

	public Connection getConnection();	
//	public <T> T getAllFieldHbase(String hbaseTable, String rowkey, Class<?> classConfig);
	
	T getAllFieldHbase(String hbaseTable, String rowkey, Class<T> classConfig) throws InstantiationException, IllegalAccessException;
}
