package br.com.vvdatalab.service;

import static org.apache.spark.sql.functions.concat_ws;

import java.util.Properties;

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
		Dataset<Row> dsSqlServer = getSqlServerData();
		
		Dataset<Row> dsHbase = dsSqlServer.withColumn("rowkey", concat_ws("-", IngestionUtils.getColumnKey(hbaseConfig.getKey())));
		
		hbaseDAO.putAll(dsHbase, dsHbase.columns(), hbaseConfig);
	}
	
	public Dataset<Row> getSqlServerData(){
		String jdbcSqlConnStr = String.format("jdbc:sqlserver://%s;database=%s;user=%s;password=%s",hbaseConfig.getServer(),hbaseConfig.getDatabase(),hbaseConfig.getUser(),hbaseConfig.getPassword());
		
		Properties properties = new Properties();
		properties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
		properties.setProperty("ApplicationIntent", "ReadOnly");
		String tableQuery = String.format("(%s) as tb",hbaseConfig.getQuery());
		
		Dataset<Row> ds = sparkSession.read().jdbc(jdbcSqlConnStr, tableQuery , properties);
		return ds;
	}
}
