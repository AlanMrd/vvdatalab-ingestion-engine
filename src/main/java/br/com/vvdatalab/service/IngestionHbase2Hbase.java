//package br.com.vvdatalab.service;
//
//import static org.apache.spark.sql.functions.concat_ws;
//
//import java.util.List;
//
//import org.apache.log4j.Logger;
//import org.apache.spark.sql.AnalysisException;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import br.com.vvdatalab.dataaccess.HBaseDAO;
//import br.com.vvdatalab.dto.HbaseConfig;
//import br.com.vvdatalab.dto.Pedidos;
//import br.com.vvdatalab.util.IngestionUtils;
//
//public class IngestionHbase2Hbase implements IngestionService{
//	
//	private HbaseConfig hbaseConfig;
//	private SparkSession sparkSession;
//	private HBaseDAO hbaseDAO;
//	
//	public IngestionHbase2Hbase(HBaseDAO hbaseDAO, HbaseConfig hbaseConfig, SparkSession sparkSession) {
//		this.sparkSession = sparkSession;
//		this.hbaseConfig = hbaseConfig;
//		this.hbaseDAO = hbaseDAO;
//	}
//	
//	@Override
//	public void executeIngestion() throws InstantiationException, IllegalAccessException {
//		
//		List<Pedidos> scanRow = hbaseDAO.scanRow("2016-01-01".getBytes(), "2016-01-10".getBytes(),Pedidos.class);
//		
//		Dataset<Row> dfPedidos = sparkSession.createDataFrame(scanRow, Pedidos.class);
//		
//		try {
//			dfPedidos.createTempView("tb");
//		} catch (AnalysisException e) {
//			e.printStackTrace();
//		}
//		
//		Dataset<Row> dsPedidos = sparkSession.sql("select DATE_FORMAT(datacompra,'yyyy-MM-dd') as datacompra,loja,sku,canalvenda,funil_vendas,funil_pedidos,aprovados,reprovados,sum(gmv) as gmv,count(idcompra) as pedidos from tb group by DATE_FORMAT(datacompra, 'yyyy-MM-dd'),loja,sku,canalvenda,funil_vendas,funil_pedidos,aprovados,reprovados order by datacompra");
//		Dataset<Row> dsHbase = dsPedidos.withColumn("rowkey", concat_ws("-", IngestionUtils.getColumnKey(hbaseConfig.getKey())));
//		
//		hbaseDAO.putAll(dsHbase, dsPedidos.columns(), hbaseConfig);
//	}
//
//}
