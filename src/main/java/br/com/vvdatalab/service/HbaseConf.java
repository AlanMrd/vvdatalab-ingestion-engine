
package br.com.vvdatalab.service;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConf implements Serializable {

	private static final long serialVersionUID = 1L;
	
	Configuration conf;
	Connection connection;
	
	public HbaseConf() {
		this.conf = HBaseConfiguration.create();
		
		try {
			this.connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Configuration getConf() {
		return conf;
	}
	

	public Connection getConnection() {
		return connection;
	}
	
}
