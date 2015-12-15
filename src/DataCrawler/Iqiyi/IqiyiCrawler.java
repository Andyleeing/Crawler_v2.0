package DataCrawler.Iqiyi;

import hbase.HBaseCRUD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import Utils.JDBCConnection;

public class IqiyiCrawler {
	public long time;
	public String date;
	public JDBCConnection jdbc;
	public IqiyiCrawler(long time, String date,JDBCConnection jdbc ) {
		this.time=time;
		this.date=date;
		this.jdbc=jdbc;
	}
	
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static ArrayList<String> allurl = new ArrayList<String>();// iqiyi404
	public dongmanFunction dongman = new dongmanFunction();
	public MovieFuction movie = new MovieFuction();
	public tvFunction tv = new tvFunction();
	public ZongYiFunction zy = new ZongYiFunction();

	
	public int crawler(String url) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(time));
	    String urls[]=url.split(" ");
	    url=urls[1];
	//    System.out.println(url);
	    try {
			if (url.contains("MvIn")) {
				return movie.infoCrawler(url, time, date,sd,jdbc);
			} else if (url.contains("MvPh")) {
				return movie.infohCrawler(url, time, date,sd,jdbc);
			} else if (url.contains("TvIn")) {
				return tv.infoCrawler(url, time, date,sd,jdbc);
			} else if (url.contains("TvIt")) {
				return tv.playCrawler(url, time, date,sd,jdbc);
			} else if (url.contains("DmIn")) {
				return dongman.infoCrawler(url, time, date,sd,jdbc);
			} else if (url.contains("DmIt")) {
				return dongman.playCrawler(url, time, date,sd,jdbc);
			} else if (url.contains("ZyIn")) {
				return zy.infoCrawler(url, time, date,sd,jdbc);
			} else {
				return zy.playCrawler(url, time, date,sd,jdbc);
			}
	    }catch(Exception e) {
	    	e.printStackTrace();
	    }
	    return 1;
	}

	
}
