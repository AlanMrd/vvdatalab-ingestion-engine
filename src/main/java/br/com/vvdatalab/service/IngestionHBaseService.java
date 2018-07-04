package br.com.vvdatalab.service;

import static org.apache.spark.sql.functions.concat_ws;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.HBaseDAO;
import br.com.vvdatalab.dto.HbaseConfig;
import br.com.vvdatalab.util.IngestionUtils;

public class IngestionHBaseService implements IngestionService {
	private HbaseConfig hbaseConfig;
	private SparkSession sparkSession;
	private HBaseDAO hbaseDAO;
	
	public IngestionHBaseService(HBaseDAO hbaseDAO, HbaseConfig hbaseConfig, SparkSession sparkSession) {
		this.sparkSession = sparkSession;
		this.hbaseConfig = hbaseConfig;
		this.hbaseDAO = hbaseDAO;
	}

	@Override
	public void executeIngestion() {		
		Dataset<Row> sql = sparkSession.sql(hbaseConfig.getQuery());
		Dataset<Row> dsHbase = sql.withColumn("rowkey", concat_ws("-", IngestionUtils.getColumnKey(hbaseConfig.getKey())));
		
		hbaseDAO.putAll(dsHbase, sql.columns());
	}
}
