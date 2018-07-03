package br.com.vvdatalab.host;

import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.HBaseDAO;
import br.com.vvdatalab.dataaccess.HBaseDAOImpl;
import br.com.vvdatalab.di.IngestionFactory;
import br.com.vvdatalab.di.SparkSessionFactory;
import br.com.vvdatalab.dto.HBaseIngestionParam;
import br.com.vvdatalab.dto.HbaseConfig;
import br.com.vvdatalab.dto.IngestionParams;
import br.com.vvdatalab.service.IngestionService;

public class IngestionEngine {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		String table = args[0];

		if (table == null) {
			throw new RuntimeException("Please, check table name");
		}

		SparkSession sparkSession = SparkSessionFactory.getSparkSession();
		
		HBaseDAO hbaseDAO = new HBaseDAOImpl();
		HbaseConfig hbaseFields = hbaseDAO.getAllFieldHbase("ingestion:properties", table, HbaseConfig.class);
		
		IngestionParams params = new HBaseIngestionParam(hbaseFields, sparkSession, hbaseDAO);
		
		IngestionService ingestionService = IngestionFactory.getIngestion(hbaseFields.getSource());
		ingestionService.executeIngestion(params);
	}
}
