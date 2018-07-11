package br.com.vvdatalab.dto;

import java.io.Serializable;

//import br.com.vvdatalab.util.VVDataUtil;

public class HbaseConfig implements Serializable{
	
private static final long serialVersionUID = 5642655208923252660L;
	
	private String source;
	private String database;
	private String outPath;
	private String password;
	private String server;
	private String table;
	private String typeFile;
	private String user;
	private String query;
	private String compression;
	private String modeFile;
	private String tableHive;
	private String dataBaseHive;
	private String delimiter;
	private String header;
	private String quote;
	private String quoteMode;
	private String key;
	private String queryContext;
	private String qtdTableUnion;
	private String columnFamily;
	
	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getQuote() {
		return quote;
	}

	public void setQuote(String quote) {
		this.quote = quote;
	}

	public String getQuoteMode() {
		return quoteMode;
	}

	public void setQuoteMode(String quoteMode) {
		this.quoteMode = quoteMode;
	}

	public String getInPath() {
		return inPath;
	}

	public void setInPath(String inPath) {
		this.inPath = inPath;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	private String partitionBy;

	private String inPath;
	private String file;

	public String getDataBaseHive() {
		return dataBaseHive;
	}

	public void setDataBaseHive(String dataBaseHive) {
		this.dataBaseHive = dataBaseHive;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getCompression() {
		return compression;
	}

	public void setCompression(String compression) {
		this.compression = compression;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getOutPath() {
		return outPath;
	}

	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}

	public String getPassword() {
		return password;
	}
	
//	public String getPasswordDecrypt() {
//		try {
//			return VVDataUtil.decrypt(password);
//		} catch (Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getTypeFile() {
		return typeFile;
	}

	public void setTypeFile(String typeFile) {
		this.typeFile = typeFile;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getModeFile() {
		return modeFile;
	}

	public void setModeFile(String modeFile) {
		this.modeFile = modeFile;
	}

	public String getTableHive() {
		return tableHive;
	}

	public void setTableHive(String tableHive) {
		this.tableHive = tableHive;
	}

	public String getPartitionBy() {
		return partitionBy;
	}

	public void setPartitionBy(String partitionBy) {
		this.partitionBy = partitionBy;
	}

	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	public String getQueryContext() {
		return queryContext;
	}

	public void setQueryContext(String queryContext) {
		this.queryContext = queryContext;
	}
	
	public String getQtdTableUnion() {
		return qtdTableUnion;
	}
	
	public void setQtdTableUnion(String qtdTableUnion) {
		this.qtdTableUnion = qtdTableUnion;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	@Override
	public String toString() {
		return "HbaseConfig [source=" + source + ", database=" + database + ", outPath=" + outPath + ", password="
				+ password + ", server=" + server + ", table=" + table + ", typeFile=" + typeFile + ", user=" + user
				+ ", query=" + query + ", compression=" + compression + ", modeFile=" + modeFile + ", tableHive="
				+ tableHive + ", dataBaseHive=" + dataBaseHive + ", delimiter=" + delimiter + ", header=" + header
				+ ", quote=" + quote + ", quoteMode=" + quoteMode + ", key=" + key + ", queryContext=" + queryContext
				+ ", qtdTableUnion=" + qtdTableUnion + ", columnFamily=" + columnFamily + ", partitionBy=" + partitionBy
				+ ", inPath=" + inPath + ", file=" + file + "]";
	}
}
