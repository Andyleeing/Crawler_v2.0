package DataCrawler.Iqiyi;

import java.io.IOException;

import Utils.JDBCConnection;
import DataCrawler.CrawlerThread;
import hbase.HBaseCRUD;

public class ZongYiFunction {
	public static int i = 100;
	public HBaseCRUD hbase = new HBaseCRUD();

	public int infoCrawler(String tmpurl, long time, String date,String crawltime,JDBCConnection jdbc) throws IOException {
		StringBuffer allcontent = new StringBuffer();
		String url = tmpurl.substring(4);
	    int flag=-1;
		if (url == null || url.equals(""))
			return flag;
		if (!url.contains("lib")) {
			try {
				String urlcode = null;
				String contentt=dongmanFunction.visitURL(url);
				if(!contentt.equals("")) {
					flag=1;
				} else {
					jdbc.log("李辉", url+"+iy", 1, "iy", url, "URL访问失败", 1);
				}
				urlcode = "info------------------" + url
						+ contentt;
				
				allcontent.append(urlcode);
				int idindex = urlcode.indexOf("tvId:");
				if (idindex >= 0) { // >=0,只能说明它满足康熙来了这样的节目。其他稍后处理。
					int idend = urlcode.indexOf(",", idindex);
					String id = urlcode.substring(idindex + 5, idend)
							.replaceAll("\\s*", ""); // 得到ID。
					int qitanidin = urlcode.indexOf("qitanid=\"");
					int qitanend = urlcode.indexOf("\"", qitanidin + 10);
					String qitanid = urlcode.substring(qitanidin + 9, qitanend);

					int idstart = urlcode.indexOf("albumId");
					if (idstart >= 0) {
						String idcode = urlcode.substring(idstart); // 此处报错。
						int idst = idcode.indexOf("albumId");
						int ided = idcode.indexOf(",");
						String did = idcode.substring(idst, ided);
						String upid = did.replaceAll("\\D*", "");
						String upurl = "http://up.video.iqiyi.com/ugc-updown/quud.do?dataid="
								+ upid + "&type=1";
						String upcode = dongmanFunction.visitURL(upurl);
						dongmanFunction.Save(allcontent, upurl, "updown", url,
								upcode);
					}

					String commurl = "http://api.t.iqiyi.com/qx_api/comment/get_video_comments?aid=0&categoryid=6&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=10&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid=0&sort=hot&t=0.9669255276880189&tvid="
							+ id;
					String commcontent = null;
					try {
						commcontent = dongmanFunction.visitURL(commurl);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					Thread.sleep(i);
					dongmanFunction.Save(allcontent, commurl, "Comment", url,
							commcontent);

					String yurl = "http://api.t.iqiyi.com/qx_api/comment/review/get_review_list?aid="
							+ qitanid
							+ "&categoryid=6&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=5&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="
							+ qitanid
							+ "&sort=hot&t=0.29865295807682946&tvid=0";
					String ycontent = null;
					try {
						ycontent = dongmanFunction.visitURL(yurl);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Thread.sleep(i);
					dongmanFunction.Save(allcontent, yurl, "Ycomment", url,
							ycontent);

					
					String IqiyiRow = "Iqiyi ZyInfo" + id + "#" + time;
					if(allcontent!=null && !allcontent.equals("")) {
						String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
						String line2=allcontent.toString();
						CrawlerThread.saveData(line1, line2);
					}
					allcontent = null;
				}// 按照电影处理。

			} catch (Exception e) {
				
				e.printStackTrace();
			}
		} else { // 包含lib.fei chang liao de .
			String urlcode = null;
			String cont=dongmanFunction.visitURL(url);
			if(!cont.equals("")) {
				flag=1;
			} 
			
			urlcode = "info------------------" + url
					+cont;
			allcontent.append(urlcode);
			int idst = urlcode.indexOf("data-qitancomment-qitanid=\"");
			int idend = urlcode.indexOf("\"", idst + 28);
			String id = urlcode.substring(idst + 27, idend);
			String commurl = "http://api.t.iqiyi.com/qx_api/comment/get_video_comments?aid=0&categoryid=6&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=10&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid=0&sort=hot&t=0.9669255276880189&tvid="
					+ id;
			String commcontent = null;
			try {
				commcontent = dongmanFunction.visitURL(commurl);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			dongmanFunction.Save(allcontent, commurl, "Comment", url,
					commcontent);

			String yurl = "http://api.t.iqiyi.com/qx_api/comment/review/get_review_list?aid="
					+ id
					+ "&categoryid=6&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=5&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="
					+ id + "&sort=hot&t=0.29865295807682946&tvid=0";
			String ycontent = null;
			try {
				ycontent = dongmanFunction.visitURL(yurl);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			dongmanFunction.Save(allcontent, yurl, "Ycomment", url, ycontent);

		
			int upidst = urlcode.indexOf("data-upanddown-albumid=\"");
			int upidend = urlcode.indexOf("\"", upidst + 25);
			String upid = urlcode.substring(upidst + 24, upidend);

			String upurl = "http://up.video.iqiyi.com/ugc-updown/quud.do?type=4&dataid="
					+ upid
					+ "&userid=&flashuid=0a0c2b1370b31525ad58b13c07b534f2";

			String upcode = dongmanFunction.visitURL(upurl);
			dongmanFunction.Save(allcontent, upurl, "updown", url, upcode);
			String IqiyiRow = "Iqiyi ZyInfo" + id + "#" + time;
			if(allcontent!=null && !allcontent.equals("")) {
				String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
				String line2=allcontent.toString();
				CrawlerThread.saveData(line1, line2);
			}
			allcontent = null;
		}
		return flag;
	}

	public int playCrawler(String tempurl, long time, String date,String crawltime,JDBCConnection jdbc) throws IOException {
		StringBuffer allcontent = new StringBuffer();
		String url = null;
		String tmpurl = tempurl.substring(4);
		String parentid = null;
         int flag=-1;
		if (tmpurl == null || tmpurl.equals(""))
			return flag;
		int urlst = tmpurl.indexOf("^");
		url = tmpurl.substring(0, urlst);
		parentid = tmpurl.substring(urlst + 1);
		String movieinfocontent = "";
		String urlcode = dongmanFunction.visitURL(url);
		if(!urlcode.equals("")) {
			flag=1;
		}  else{
			jdbc.log("李辉", url+"+iy", 1, "iy", url, "URL访问失败", 1);
		}
		try {
			Thread.sleep(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		movieinfocontent = "info------------------" + url + urlcode;
		allcontent.append(movieinfocontent);
		int idstart = urlcode.indexOf("albumId");
		if (idstart > -1) {
			String idcode = urlcode.substring(idstart);
			int idst = idcode.indexOf("albumId");
			int ided = idcode.indexOf(",");
			String did = idcode.substring(idst, ided);
			String id = did.replaceAll("\\D*", "");
			int tvstart = urlcode.indexOf("tvId");
			String tvcode = urlcode.substring(tvstart);
			int tvst = tvcode.indexOf("tvId");
			int tved = tvcode.indexOf(",");
			String ttv = tvcode.substring(tvst, tved);
			String tv = ttv.replaceAll("\\D*", "");
			int qitanid = urlcode.indexOf("data-qitancomment-qitanid=\"");
			String qitanidd = null;
			if (qitanid >= 0) {
				String qitancode = urlcode.substring(qitanid);
				int qitanst = qitancode.indexOf("qitanid=\"");
				int qitanend = qitancode.indexOf("\"", qitanst + 10);
				qitanidd = qitancode.substring(qitanst + 9, qitanend);
			}
			String commurl = "http://api.t.iqiyi.com/qx_api/comment/get_video_comments?aid=0&categoryid=6&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=10&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid=0&sort=hot&t=0.9669255276880189&tvid="
					+ tv;
			String commcontent = null;
			try {
				commcontent = dongmanFunction.visitURL(commurl);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			dongmanFunction.Save(allcontent, commurl, "Comment", url,
					commcontent);

			String yurl = "http://api.t.iqiyi.com/qx_api/comment/review/get_review_list?aid="
					+ qitanidd
					+ "&categoryid=6&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=5&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="
					+ qitanidd + "&sort=hot&t=0.29865295807682946&tvid=0";
			String ycontent = null;
			try {
				ycontent = dongmanFunction.visitURL(yurl);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			dongmanFunction.Save(allcontent, yurl, "Ycomment", url, ycontent);
			
			String precentge = "http://cache.video.qiyi.com/pc/pr/" + id
					+ "/playCountPCMobileCb?callback=playCountPCMobileCb";
			String precode = null;
			precode = dongmanFunction.visitURL(precentge);
			dongmanFunction.Save(allcontent, precentge, "Precentge", url,
					precode);

			String reference = "http://mixer.video.iqiyi.com/jp/recommend/videos?referenceId="
					+ tv
					+ "&albumId="
					+ id
					+ "&channelId=6&cookieId=c15080ccea46c39cb733eec29be0e154&withRefer=false&area=bee&size=12&type=video&pru=2145856022&callback=window.Q.__callbacks__.cb9d67pj";
			String refercode = null;
			refercode = dongmanFunction.visitURL(reference);
			dongmanFunction.Save(allcontent, reference, "reference", url,
					refercode);

			String sumplaycount = "http://cache.video.qiyi.com/jp/pc/" + id
					+ "/?callback=window.Q.__callbacks__.cbgt6rz7";
			String summcode = null;
			summcode = dongmanFunction.visitURL(sumplaycount);
			dongmanFunction.Save(allcontent, sumplaycount, "Sumplaycount", url,
					summcode);

			String upurl = "http://up.video.iqiyi.com/ugc-updown/quud.do?dataid="
					+ tv + "&type=2";
			String upcode = null;
			upcode = dongmanFunction.visitURL(upurl);
			dongmanFunction.Save(allcontent, upurl, "updown", url, upcode);

			String IqiyiRow = "Iqiyi ZyItem" + id + "#" + parentid + "@"+time;
			if(allcontent!=null && !allcontent.equals("")) {
				String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
				String line2=allcontent.toString();
				CrawlerThread.saveData(line1, line2);
			}
			allcontent = null;
		}
           return flag;
	}
}
