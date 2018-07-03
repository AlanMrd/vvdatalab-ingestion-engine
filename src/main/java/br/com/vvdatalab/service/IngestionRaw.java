package br.com.vvdatalab.service;

import org.apache.spark.sql.SparkSession;
import br.com.vvdatalab.dataaccess.Hbase;
import br.com.vvdatalab.dto.HbaseConfig;

public class IngestionRaw implements Ingestion {

	@Override
	public void executeIngestion(HbaseConfig hbaseFields,SparkSession sparkSession, Hbase hbase) {
		// TODO Auto-generated method stub
		
	}
}
