package br.com.vvdatalab.di;

import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.HBaseDAO;
import br.com.vvdatalab.dto.HbaseConfig;
import br.com.vvdatalab.service.IngestionHBaseService;
import br.com.vvdatalab.service.IngestionRawService;
import br.com.vvdatalab.service.IngestionService;

public class IngestionFactory {
	
	private HbaseConfig hbaseConfig;
	private SparkSession sparkSession;
	private HBaseDAO hbaseDAO;
	
	public IngestionFactory(HBaseDAO hbaseDAO, HbaseConfig hbaseFields, SparkSession ss) {
		this.sparkSession = ss;
		this.hbaseConfig = hbaseFields;
		this.hbaseDAO = hbaseDAO;
	}
		
	public IngestionService getIngestion() {
		String ingestion = hbaseConfig.getSource();
		
		if(ingestion == null) {
			return null;
		}
		else if(ingestion.equalsIgnoreCase("HBASE")){
			return new IngestionHBaseService(hbaseDAO, hbaseConfig, sparkSession);
		}
		else if(ingestion.equalsIgnoreCase("SQL")){
			return new IngestionRawService();
		}
		else{
			return null;
		}
	}
}
