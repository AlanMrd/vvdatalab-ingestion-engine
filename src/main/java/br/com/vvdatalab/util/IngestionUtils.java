package br.com.vvdatalab.util;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.collection.JavaConversions;
import scala.collection.mutable.Seq;
import scala.reflect.internal.Trees.New;

public class IngestionUtils {

	public static Seq<Column> getColumnKey(String key) {
		String[] splitKey = key.split(",");
		List<Column> listKey = new ArrayList<>();

		for (String fieldKey : splitKey) {
			listKey.add(col(fieldKey));
		}

		return JavaConversions.asScalaBuffer(listKey).seq();
	}

	public static String[] colLowerCase(String[] columns) {
		List<String> newColumns = new ArrayList<>();

		for (String column : columns) {
			newColumns.add(column.toLowerCase());
		}

		String[] renamedColumns = new String[newColumns.size()];

		return newColumns.toArray(renamedColumns);
	}
}
