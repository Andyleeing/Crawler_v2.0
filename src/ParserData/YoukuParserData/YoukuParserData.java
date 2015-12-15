package ParserData.YoukuParserData;

import hbase.HBaseCRUD;
import Utils.JDBCConnection;

public class YoukuParserData {
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	public ParserData.YoukuParserData.Parser movie;
	public ParserData.YoukuParserData.TVParser TV; 
	public ParserData.YoukuParserData.DongmanParser dongman; 
	public ParserData.YoukuParserData.ZYParser zy;
	
	public YoukuParserData(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
		movie = new Parser(hbase,jdbconn);
		TV = new TVParser(hbase,jdbconn);
		dongman = new DongmanParser(hbase,jdbconn);
		zy = new ZYParser(hbase,jdbconn);
	}
	
	public void resolveData(String attibute,String content) {
		String[] splits = attibute.split(" ");
		if(splits.length < 6)
			return;
		try {
			if(splits[1].equals("info")) {
				if(splits[2].equals("movie")) {
					movie.infoParser(content, splits[3], splits[4], splits[5], "movie");
				}else if(splits[2].equals("dongman")) {
					dongman.infoParser(content, splits[3], splits[4], splits[5], "dongman");
				}else if(splits[2].equals("tv")) {
					TV.infoParser(content, splits[3], splits[4], splits[5], "tv");
				}else if(splits[2].equals("zongyi")) {
					zy.infoParser(content, splits[3], splits[4], splits[5], "zongyi");
				}
			} else if(splits[1].equals("play")) {
				if(splits.length < 7)
					return;
				if(splits[2].equals("movie")) {
					movie.playParser(content, splits[3], splits[5], splits[4],splits[6], "movie");
				}else if(splits[2].equals("dongman")) {
					dongman.playParser(content,splits[3], splits[5], splits[4],splits[6], "dongman");
				}else if(splits[2].equals("tv")) {
					TV.playParser(content, splits[3], splits[5], splits[4],splits[6], "tv");
				}else if(splits[2].equals("zongyi")) {
					zy.playParser(content, splits[3], splits[5], splits[4],splits[6], "zongyi");
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
