package ParserData.Iqiyi;

import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import Utils.JDBCConnection;

public class IqiyiParse {

	public static BufferedReader br;
	public static ArrayList<IqiyiParse> pool;

	
	public static String parentId;
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	public static String crawltime;

	

	public IqiyiParse(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}

	
	public void iyparse(String attribute,String content) {
	     int index=attribute.indexOf("#");
	     int end=attribute.indexOf(" ",index);
		String timestamp=attribute.substring(index+1,end);
		String sd=attribute.substring(attribute.indexOf("@@")+2);
		attribute=attribute.replaceAll("Iqiyi ","");
		int firstspare = attribute.indexOf(" ");
		int secendspare = attribute.indexOf(" ", firstspare + 2);
	//	Id = attribute.substring(firstspare + 1, secendspare);
		int TypeEnd = attribute.indexOf("#");
		int intime = attribute.indexOf("@");
		if (TypeEnd == -1) {
	//		System.out.println(attribute);
		}
		String sType = attribute.substring(0, TypeEnd);
		String Type = sType.replaceAll("[0-9]*", "");
	//	System.out.println(Type);
		if (Type.contains("Item")) {
			parentId = attribute.substring(TypeEnd + 1, intime); // 父URL。
			    index=attribute.indexOf("@");
			    end=attribute.indexOf(" ",index);
			    timestamp=attribute.substring(index+1,end);		
		}
		
		parser parser = new parser(hbase,jdbconn,timestamp,sd);
		int indexcrawl=attribute.indexOf("@@");
		if(indexcrawl>=0) {
		crawltime=attribute.substring(indexcrawl+2);
		}
		if (Type.equals("ZhengPian")) {
			parser.Movie(content,jdbconn);
		} else if (Type.equals("PianHua")) {
			parser.MoviePh(content,jdbconn);
		} else if (Type.equals("TvInfo")) {
			parser.TvInfo(content,jdbconn);
		} else if (Type.equals("DongManInfo")) {
			parser.DongManInfo(content,jdbconn);
		} else if (Type.equals("TvItem")) {
			parser.TvItem(content);
		} else if (Type.equals("DongManItem")) {
			parser.DongManItem(content);
		} else if (Type.equals("ZyInfo")) {
			parser.ZongYiInfo(content);
		} else if (Type.equals("ZyItem")) {
			parser.ZongYiItem(content,jdbconn);
		}
		parser.referencemovie();
		parser.referencevideo();
	}
	
	
}
