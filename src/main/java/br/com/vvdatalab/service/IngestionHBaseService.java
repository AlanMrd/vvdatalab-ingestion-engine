package br.com.vvdatalab.service;

import static org.apache.spark.sql.functions.concat_ws;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import br.com.vvdatalab.dto.HBaseIngestionParam;
import br.com.vvdatalab.dto.IngestionParams;
import br.com.vvdatalab.util.IngestionUtils;

public class IngestionHBaseService implements IngestionService {
	
	@Override
	public void executeIngestion(IngestionParams params) {
		HBaseIngestionParam hbaseParams = (HBaseIngestionParam) params;
		
		Dataset<Row> sql = hbaseParams.getSparkSession().sql(hbaseParams.getHbaseFields().getQuery());
		Dataset<Row> dsHbase = sql.withColumn("rowkey", concat_ws("-", IngestionUtils.getColumnKey(hbaseParams.getHbaseFields().getKey())));
		
		hbaseParams.getHbaseDAO().putAll(dsHbase, sql.columns());
	}
}
