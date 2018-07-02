package br.com.vvdatalab.di;

import br.com.vvdatalab.service.Ingestion;
import br.com.vvdatalab.service.IngestionHbase;
import br.com.vvdatalab.service.IngestionRaw;

public class IngestionFactory {
	
	public Ingestion getIngestion(String ingestion) {
		if(ingestion == null) {
			return null;
		}
		else if(ingestion.equalsIgnoreCase("HBASE")){	
			return new IngestionHbase();
		}
		else if(ingestion.equalsIgnoreCase("SQL")){
			return new IngestionRaw();
		}
		else{
			return null;
		}
	}
}
