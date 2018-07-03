package br.com.vvdatalab.host;

import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.Hbase;
import br.com.vvdatalab.dataaccess.HbaseImpl;
import br.com.vvdatalab.di.IngestionFactory;
import br.com.vvdatalab.dto.HbaseConfig;
import br.com.vvdatalab.service.Ingestion;

public class IngestionEngine {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		String table = args[0];

		if (table == null) {
			throw new RuntimeException("Please, check table name");
		}

		SparkSession ss = SparkSession.builder().appName("data_import").config("spark.dynamicAllocation.enabled", true)
				.config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
				.config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();
		
		Hbase hbase = new HbaseImpl();
		HbaseConfig hbaseFields = hbase.getAllFieldHbase("ingestion:properties", table, HbaseConfig.class);
		
		IngestionFactory factoryIngestion = new IngestionFactory();
	
		Ingestion ingestion = factoryIngestion.getIngestion(hbaseFields.getSource());
		ingestion.executeIngestion(hbaseFields, ss, hbase);
	}

}
