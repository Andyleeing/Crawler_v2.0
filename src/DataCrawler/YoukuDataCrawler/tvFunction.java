package DataCrawler.YoukuDataCrawler;

import Utils.JDBCConnection;
import hbase.HBaseCRUD;
import DataCrawler.CrawlerThread;

public class tvFunction {
	public static int i = 100;
	public HBaseCRUD hbase = new HBaseCRUD();
	public JDBCConnection jdbc;
	public tvFunction(JDBCConnection jdbc) {
		this.jdbc = jdbc;
	}
	public int infoCrawler(String url,long time,String date) {
		StringBuffer infosb = new StringBuffer(url);
		String msrowkey =  infosb.reverse().toString()
				.substring(infosb.indexOf(".") + 1);
		String rowKey = msrowkey + time;
		String movieinfoContent = null;
		movieinfoContent = Function.visitURL(url);
		if (movieinfoContent != null &&  movieinfoContent.length() > 100) {
			CrawlerThread.saveData("youku info tv "+rowKey+" "+url+" "+date.replaceAll(" ","_"), movieinfoContent);
			movieinfoContent = null;
			return 1;
		}
		
		
		if(movieinfoContent != null && movieinfoContent.length() <= 5) {
			try{
				Integer.parseInt(movieinfoContent);
				jdbc.log("hanjiangxue", msrowkey + "+yk", 1, "yk", url, movieinfoContent, 1);
			} catch(Exception e) {
			}
		} else
			jdbc.log("hanjiangxue", msrowkey, 1, "yk", url, "no content", 1);
		
		return -1;
	}

	public int playCrawler(String url,long time,String date) {
		String[] splits = url.split(" ");
		StringBuffer playsb = new StringBuffer(splits[0]);
	
		String playRowMS = playsb.reverse().toString()
				.substring(playsb.indexOf("lmth.") + 5);
		String playRow = playRowMS + time;
		
		String Content = null;
		Content = Function.visitURL(splits[0]);
		
		if(Content != null && Content.length() <= 5) {
			try{
				Integer.parseInt(Content);
				jdbc.log("hanjiangxue", playRowMS + "+yk", 1, "yk", url, Content, 1);
			} catch(Exception e) {
			}
			return -1;
		}
		
		// ////////////
		if (Content != null && !Content.equals("")) {
			int index = Content.indexOf("videoId");
			int index1 = Content.indexOf("showid");
			if(index > -1 && index1 > -1)
			{
				String videoid = "";
				String showid = Content.substring(index1 + 8);
				
				videoid = Content.substring(index + 11);
				videoid = videoid.substring(0, videoid.indexOf("'"));
				
				showid = showid.substring(0, showid.indexOf("\""));
				
				String sumplayCountURL = "http://v.youku.com/QVideo/~ajax/getVideoPlayInfo?_rt=1&_ro=&id=" + videoid + "&sid=" + showid + "&type=vv&catid=97";
				String sumplayCountContent = Function.visitURL(sumplayCountURL);
				Content += sumplayCountContent;
				
				if(sumplayCountContent != null && sumplayCountContent.length() <= 5) {
					try{
						Integer.parseInt(sumplayCountContent);
						jdbc.log("hanjiangxue", playRowMS + "+yk", 1, "yk", url, sumplayCountContent, 1);
					} catch(Exception e) {
					}
					return -1;
				}
				
				String dynamic = "http://v.youku.com/v_vpactionInfo/id/" + videoid
						+ "/";
				String dynamicContent = Function.visitURL(dynamic);
				Content += "*@@@*" + dynamicContent;
				
				
				String relate = "http://ykrec.youku.com/show/packed/list.json?vid="
					+ videoid + "&sid=" + showid
					+ "&cate=97&apptype=1&pg=3&module=1&pl=6";
				String relateContent = Function.visitURL(relate);
				Content += "*$$$*" + relateContent;
			}
		}
		if (Content != null && Content.length() > 100) {
			CrawlerThread.saveData("youku play tv "+playRow+" "+url+" "+date.replaceAll(" ","_"), Content);
			Content = null;
			return 1;
		}
		
		jdbc.log("hanjiangxue", playRowMS, 1, "yk", url, "no content", 1);
		return -1;
	}
}
