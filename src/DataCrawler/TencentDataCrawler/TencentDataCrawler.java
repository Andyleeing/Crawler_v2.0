package DataCrawler.TencentDataCrawler;

import java.io.FileWriter;

import Utils.JDBCConnection;

public class TencentDataCrawler {
	
public static FileWriter fw;
public JDBCConnection jdbc=new JDBCConnection();

OutputFile2 opfm;
OutputFileTV2 opftv;
OutPutFilecartoon2 opfc;
OutputFileZongYi2 opfz;

public TencentDataCrawler(JDBCConnection jdbc){
	this.jdbc = jdbc;
	opfm = new OutputFile2(jdbc);
	opftv = new OutputFileTV2(jdbc);
	opfc = new OutPutFilecartoon2(jdbc);
	opfz = new OutputFileZongYi2(jdbc);
}

	public int crawler(String url,long time) {
		int flag = 1; 
		url  = url.substring(8);
		if (url.indexOf("type:movie") > 0) {
		//	OutputFile2 opf = new OutputFile2();
			opfm.tencentOutput(url, time);
		} else if (url.indexOf("type:tv") > 0) {
		//	OutputFileTV2 opf = new OutputFileTV2();
			opftv.tencentOutput(url, time);
		} else if (url.contains("type:zongyi")){
			opfz.tencentOutput(url, time);
		} else {
		//	OutPutFilecartoon2 opf = new OutPutFilecartoon2();
			opfc.tencentOutput(url, time);
		}
		return flag;
	}
}
