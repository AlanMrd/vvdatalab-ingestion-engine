package br.com.vvdatalab.dto;

import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.HBaseDAO;
import br.com.vvdatalab.dto.HbaseConfig;

public class HBaseIngestionParam extends IngestionParams {

	private HbaseConfig hbaseFields;
	private SparkSession sparkSession;
	private HBaseDAO hbaseDAO;
	
	public HBaseIngestionParam(HbaseConfig hbaseFields, SparkSession sparkSession, HBaseDAO hbaseDAO) {
		super();
		this.hbaseFields = hbaseFields;
		this.sparkSession = sparkSession;
		this.hbaseDAO = hbaseDAO;
	}

	public HbaseConfig getHbaseFields() {
		return hbaseFields;
	}

	public SparkSession getSparkSession() {
		return sparkSession;
	}

	public HBaseDAO getHbaseDAO() {
		return hbaseDAO;
	}
}
