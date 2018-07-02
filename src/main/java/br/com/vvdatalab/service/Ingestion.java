package br.com.vvdatalab.service;

import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dto.HbaseConfig;

public interface Ingestion {
	public void executeIngestion(HbaseConfig hbaseFields, SparkSession sparkSession);
}
