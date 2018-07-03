package br.com.vvdatalab.di;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

	public static SparkSession getSparkSession() {
		SparkSession sparkSession = SparkSession.builder().appName("data_import").config("spark.dynamicAllocation.enabled", true)
				.config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
				.config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();
		return sparkSession;
	}
}
