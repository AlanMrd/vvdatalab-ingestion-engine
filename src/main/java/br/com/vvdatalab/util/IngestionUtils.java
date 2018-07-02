package br.com.vvdatalab.util;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import scala.collection.JavaConversions;
import scala.collection.mutable.Seq;

public class IngestionUtils {
	
	public static Seq<Column> getColumnKey(String key){
		String[] splitKey = key.split(",");
		List<Column> listKey = new ArrayList<>();
		
		for(String fieldKey : splitKey){
			listKey.add(col(fieldKey));
		}

		return JavaConversions.asScalaBuffer(listKey).seq();
	}
}
