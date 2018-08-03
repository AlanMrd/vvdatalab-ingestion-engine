package br.com.vvdatalab.dataaccess;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dto.HbaseConfig;

public class OracleDAOImpl implements OracleDAO {

	private static final long serialVersionUID = 1L;

	@Override
	public Dataset<Row> selectHive(HbaseConfig hbaseConfig, SparkSession sparkSession) {
		String jdbcOracleConnStr = "jdbc:oracle:thin:@//10.128.132.30:1530/DLN01";

		Properties properties = new Properties();
		properties.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
		properties.setProperty("ApplicationIntent", "ReadOnly");
		properties.put("user", "svc_bigdata");
		properties.put("user", "svc_bigdata");
		properties.put("password", "J4rd1m#1qaz");
		
//		String tableQuery = String.format("(%s) tb","select * from own_mis.vw_dom_sku_departamento");
		String tableQuery = String.format("(%s) tb","select sku_dep.cd_departamento as departamento, sku_dep.ds_departamento as nomedepartamento, sku.cd_item as dep_sku from own_mis.vw_dom_sku_departamento sku_dep inner join own_mis.vw_sku sku on sku_dep.cd_departamento = sku.cd_departamento");
		
		return sparkSession.read().jdbc(jdbcOracleConnStr, tableQuery , properties);
	}

}
