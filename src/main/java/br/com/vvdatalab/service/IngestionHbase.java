package br.com.vvdatalab.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.concat_ws;
import static br.com.vvdatalab.util.IngestionUtils.getColumnKey;
import br.com.vvdatalab.dataaccess.Hbase;
import br.com.vvdatalab.dto.HbaseConfig;

public class IngestionHbase implements Ingestion {

	@Override
	public void executeIngestion(HbaseConfig hbaseFields,SparkSession sparkSession) {
		Dataset<Row> sql = sparkSession.sql(hbaseFields.getQuery());
		Dataset<Row> dsHbase = sql.withColumn("rowkey", concat_ws("-", getColumnKey(hbaseFields.getKey())));
		
//		this..putAll();
	

	}

}
