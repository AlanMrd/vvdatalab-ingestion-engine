package br.com.vvdatalab.di;

import br.com.vvdatalab.service.IngestionHBaseService;
import br.com.vvdatalab.service.IngestionRawService;
import br.com.vvdatalab.service.IngestionService;

public class IngestionFactory {
		
	public static IngestionService getIngestion(String ingestion) {
		if(ingestion == null) {
			return null;
		}
		else if(ingestion.equalsIgnoreCase("HBASE")){	
			return new IngestionHBaseService();
		}
		else if(ingestion.equalsIgnoreCase("SQL")){
			return new IngestionRawService();
		}
		else{
			return null;
		}
	}
}
