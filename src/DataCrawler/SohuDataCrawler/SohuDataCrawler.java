package DataCrawler.SohuDataCrawler;

import DataCrawler.CrawlerThread;
import DataCrawler.Iqiyi.MovieFuction;
import Utils.JDBCConnection;

public class SohuDataCrawler {
	public JDBCConnection jdbc;
	public long time;
	public SohuDataCrawler(JDBCConnection jdbc,long time){
		this.jdbc = jdbc;
		this.time=time;
	}
	public UtilsSohu ulsh = new UtilsSohu();
	public CrawlDynamic cl = new CrawlDynamic();
	
	public int  crawler(String url,long time)  {
 		int flag = 1; 
		if (url == null || url.equals("")) {
			return -1;
		}
	     String[]two=url.split(" ");
	     if(two==null||two.length<2)return -1;
		String[] items = two[1].split("@");
		// url category type [infokey]
		if (items.length == 3) {
			infoCrawler(items[0], items[1], time);
		} 
		else if (items.length == 4) {
		if(items[0].indexOf("###paid")>=0){
				paidCrawler(items[0],items[1],items[3], time);
		}
		else {
     		playCrawler(items[0], items[1], items[3],time);
		}
		}
		return flag;
		
	}
	
	public  void paidCrawler(String url,String category,String infokey,long time) {
		String[] id = new String[3];
	    url=url.substring(0, url.indexOf("###"));
		double sumplay=ulsh.paidSumplay(infokey);
		long sumplayCount=(long)sumplay;
		id = ulsh.getVPid(url, category,jdbc);
		if (id == null)	return;
		String vid, playlistid, source;
		vid = id[0];
		playlistid = id[1];
		source = id[2];
		String items[] = new String[5];
		items = cl.getPaidPlay(category, vid, playlistid);
		if (items == null)
			return;
		String rowkey = null;
		rowkey = UtilsSohu.getRowkey(url);
		if (rowkey == null)
			return;
		String attribute = "sohu " +"@@"+ rowkey + "@@play" + "@@" + url + "@@" + time + "@@"
				+ "paid" +"@@"+infokey ;
		String content= source + "*@@@*" + items[0]
				+ "*@@@*" + items[1]+ "*@@@*"+sumplayCount;
		CrawlerThread.saveData(attribute, content);

	}
	
	public  void playCrawler(String url, String category, String infokey,long time)  {
		String[] id = new String[3];
		id = ulsh.getVPid(url, category, jdbc);
		if (id == null)	return;
		String vid, playlistid, source;
		vid = id[0];
		playlistid = id[1];
		source = id[2];
		String items[] = new String[5];
		items = cl.getPlay(category, vid, playlistid);
		if (items == null)return;
		String rowkey = null;
		rowkey = UtilsSohu.getRowkey(url);
		if (rowkey == null)
			return;
		String last = "";
		if (items.length >= 5)
			last = items[4];
		String attribute = "sohu " +"@@"+ rowkey + "@@play" + "@@" + url + "@@" + time + "@@"
				+ category + "@@" + infokey;
		String content= source + "*@@@*" + items[0]
				+ "*@@@*" + items[1] + "*@@@*" + items[2] + "*@@@*" + items[3]
				+ "*@@@*" + last ;
		CrawlerThread.saveData(attribute, content);

	}
	public void infoCrawler(String url, String category,long time){
		String[] id = new String[3];
		id = ulsh.getVPid(url, category, jdbc);
		if (id == null)	return;
		String playlistid, source;
		playlistid = id[1];
		source = id[2];
		String items[] = new String[2];
		items = cl.getInfo(category, playlistid);
		if (items == null)return;
		String[] counts=new String[2];
		String rowkey = null;
		rowkey = UtilsSohu.getRowkey(url);
		if (rowkey == null)
			return;
		String attribute;
		String content;
		if (category.indexOf("dianying")>=0) {
			counts= UtilsSohu.movieInfoCount(source);
			if(counts==null)return;
		    attribute = "sohu " +"@@"+ rowkey + "@@info" + "@@" + url + "@@" + time + "@@"
					+ category;
		   content= source + "*@@@*" + items[0] + "*@@@*"
					+ counts[0] + "*@@@*"+counts[1];
		}
		
		else {
			String playcountString=cl.getInfoCount(playlistid);
			attribute = "sohu " +"@@"+ rowkey + "@@info" + "@@" + url + "@@" + time + "@@"
				+ category;
			content=source + "*@@@*" + items[0] + "*@@@*"
				+ items[1] + "*@@@*"+playcountString ;
		}
		CrawlerThread.saveData(attribute, content);
	}

}
