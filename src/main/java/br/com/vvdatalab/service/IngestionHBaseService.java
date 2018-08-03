package br.com.vvdatalab.service;

import static org.apache.spark.sql.functions.concat_ws;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dataaccess.HBaseDAO;
import br.com.vvdatalab.dataaccess.OracleDAO;
import br.com.vvdatalab.dataaccess.OracleDAOImpl;
import br.com.vvdatalab.dataaccess.SqlServerDAO;
import br.com.vvdatalab.dataaccess.SqlServerDAOImpl;
import br.com.vvdatalab.dto.HbaseConfig;
import br.com.vvdatalab.util.IngestionUtils;

public class IngestionHBaseService implements IngestionService {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private HbaseConfig hbaseConfig;
	private SparkSession sparkSession;
	private HBaseDAO hbaseDAO;
	private SqlServerDAO sqlServerDAO;
	private OracleDAO oracleDAO;
	
	public IngestionHBaseService(HBaseDAO hbaseDAO, HbaseConfig hbaseConfig, SparkSession sparkSession) {
		this.sparkSession = sparkSession;
		this.hbaseConfig = hbaseConfig;
		this.hbaseDAO = hbaseDAO;
		this.sqlServerDAO = new SqlServerDAOImpl();
		this.oracleDAO = new OracleDAOImpl();
	}

	@Override
	public void executeIngestion() {				
		Dataset<Row> dsSqlServer = sqlServerDAO.selectHive(hbaseConfig, sparkSession);
		Dataset<Row> dsOracle = oracleDAO.selectHive(hbaseConfig, sparkSession);
		
		Dataset<Row> joinDataBase = dsSqlServer.join(dsOracle, dsSqlServer.col("sku").equalTo(dsOracle.col("dep_sku")), "inner");
		
		Dataset<Row> dsHbase = joinDataBase.withColumn("rowkey", concat_ws("-", IngestionUtils.getColumnKey(hbaseConfig.getKey())));
	
		Dataset<Row> df = dsHbase.toDF(IngestionUtils.colLowerCase(dsHbase.columns())).drop(new String[]{"dep_sku"});
		
		System.out.println("Dataset " + df.count());
		
//		try {
//			hbaseDAO.putAll(df, df.columns(), hbaseConfig);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	} 	
}
