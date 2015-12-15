package ParserData.TencentParserData;

import hbase.HBaseCRUD;
import Utils.JDBCConnection;

public class TencentParserData {
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	PlayParserZongYi ppzy ;
	playParserTV pptv;
	public TencentParserData(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
		ppzy= new PlayParserZongYi(hbase,jdbconn);
		pptv = new playParserTV(hbase,jdbconn);
	}
	
	public void resolveDate(String info, String content) {
		if ( info.contains("/") || info.contains("zongyi")){
			if (!info.contains("type")) {
				ppzy.analyseInfo(content);
			} else {
				ppzy.analysePlay(content);
			}
			return;
		}
		info = info.substring(8);
		int mtype = info.indexOf("+");
		String cId = info.substring(0,15);	
		String vId = "";	
		if (mtype > 0){ 
			vId = info.substring(mtype + 1 , mtype + 12);	
		} //判断是info还是play, <0是info页			
		pptv.analyze(content, mtype, cId, vId);	
	}
}
