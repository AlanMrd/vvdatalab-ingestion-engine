package br.com.vvdatalab.host;

import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.HBaseDAO;
import br.com.vvdatalab.dataaccess.HBaseDAOImpl;
import br.com.vvdatalab.di.IngestionFactory;
import br.com.vvdatalab.dto.HbaseConfig;
import br.com.vvdatalab.service.IngestionService;

public class IngestionEngine {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		String table = args[0];

		if (table == null) {
			throw new RuntimeException("Please, check table name");
		}

		SparkSession sparkSession = SparkSession.builder().appName("data_import").config("spark.dynamicAllocation.enabled", true)
				.config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
				.config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();

		
		HBaseDAO hbaseDAO = new HBaseDAOImpl();
		HbaseConfig hbaseFields = hbaseDAO.getAllFieldHbase("ingestion:properties", table, HbaseConfig.class);
		System.out.println(hbaseFields);
	
		IngestionFactory ingestionFactory = new IngestionFactory(hbaseDAO, hbaseFields, sparkSession); 
		IngestionService ingestion = ingestionFactory.getIngestion();
		ingestion.executeIngestion();
	}
}
