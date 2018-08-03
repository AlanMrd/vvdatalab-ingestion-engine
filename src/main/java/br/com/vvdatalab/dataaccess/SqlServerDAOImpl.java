package br.com.vvdatalab.dataaccess;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dto.HbaseConfig;

public class SqlServerDAOImpl implements SqlServerDAO{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Dataset<Row> selectHive(HbaseConfig hbaseConfig, SparkSession sparkSession) {
		String jdbcSqlConnStr = String.format("jdbc:sqlserver://%s;database=%s;user=%s;password=%s",hbaseConfig.getServer(),hbaseConfig.getDatabase(),hbaseConfig.getUser(),hbaseConfig.getPassword());
		
		Properties properties = new Properties();
		properties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
		properties.setProperty("ApplicationIntent", "ReadOnly");
		String tableQuery = String.format("(%s) as tb",hbaseConfig.getQuery());
		
		return sparkSession.read().jdbc(jdbcSqlConnStr, tableQuery , properties);
	}

}
