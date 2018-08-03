package br.com.vvdatalab.dataaccess;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import br.com.vvdatalab.dto.HbaseConfig;

public interface OracleDAO extends Serializable {
	Dataset<Row> selectHive(HbaseConfig hbaseConfig, SparkSession sparkSession);
}
