package br.com.vvdatalab.host;

import br.com.vvdatalab.dataaccess.Hbase;
import br.com.vvdatalab.dataaccess.HbaseImpl;
import br.com.vvdatalab.dto.HbaseConfig;

public class IngestionEngine {
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		String table = args[0];
		
		if(table == null){
			throw new RuntimeException("Please, check table name");
		}
		
		Hbase<HbaseConfig> hbase = new HbaseImpl<>();
		HbaseConfig allFieldHbase = hbase.getAllFieldHbase("ingestion:properties", "CLIENTE-EXTRA", HbaseConfig.class);
		String dataBaseHive = allFieldHbase.getDataBaseHive();
		System.out.println(dataBaseHive);
	}

}
