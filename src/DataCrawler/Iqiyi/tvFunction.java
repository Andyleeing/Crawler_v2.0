package DataCrawler.Iqiyi;

import hbase.HBaseCRUD;

import java.io.IOException;

import Utils.JDBCConnection;
import DataCrawler.CrawlerThread;



public class tvFunction {
	public static int i = 100;
	public HBaseCRUD hbase = new HBaseCRUD();

	public int infoCrawler(String tmpurl, long time, String date,String crawltime,JDBCConnection jdbc) throws IOException {
		StringBuffer allcontent = new StringBuffer();
		String url = tmpurl.substring(4);
	    int flag=-1;
		if (url == null || url.equals(""))
			return flag;
		if (url.contains("jhx") == false) {
			String movieinfocontent = "";
			String movieincon = null;
			try {
				movieincon = dongmanFunction.visitURL(url);
				if(movieincon !=null) {
				flag=1;
				} 
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			movieinfocontent = "info------------------" + url + movieincon;
			allcontent.append(movieinfocontent);
			if (url.contains("/v_") == false
					&& url.matches(".*/n[0-9]*.html.*") == false
					&& url.matches(".*/[0-9][0-9].*") == false
					&& url.matches(".*/[a-z][0-9].*") == false) { // 带有info页的TV。
				int idstart = movieinfocontent.indexOf("albumId:");
				if (idstart >= 0) {
					String idcode = movieinfocontent.substring(idstart); // 此处报错。

					int idst = idcode.indexOf("albumId");
					int ided = idcode.indexOf(",");
					String did = idcode.substring(idst, ided);
					String id = did.replaceAll("\\D*", "");
					String upscore = "ttp://up.video.iqiyi.com/ugc-updown/quud.do?dataid="
							+ id
							+ "&type=1&userid=2145856022&flashuid=a37ddc42a3dac4f4ed0f36f28f5db50c&callback=window.Q.__callbacks__.cbhdqq1s";
					String upscocon = dongmanFunction.visitURL(upscore);
					allcontent.append(upscocon);
					int tvstart = movieinfocontent.indexOf("tvId:");
					String tvcode = movieinfocontent.substring(tvstart);
					int tvst = tvcode.indexOf("tvId");
					int tved = tvcode.indexOf(",");
					String ttv = tvcode.substring(tvst, tved);
					String tv = ttv.replaceAll("\\D*", "");
					String qitanidd = "";
					int qitanid = movieinfocontent.indexOf("qitanid=");
					if (qitanid >= 0) {
						String qitancode = movieinfocontent.substring(qitanid);
						int qitanst = qitancode.indexOf("qitanid=");
						int qitaned = qitancode.indexOf("\"", qitanst + 9);
						String qitan = qitancode.substring(qitanst, qitaned);
						qitanidd = qitan.replaceAll("\\D*", "");
					} else {
						qitanidd = "0";
					}

					String precentge = "http://cache.video.qiyi.com/pc/pr/"
							+ id
							+ "/playCountPCMobileCb?callback=playCountPCMobileCb";
					String preContent = dongmanFunction.visitURL(precentge);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					dongmanFunction.Save(allcontent, precentge, "precentge",
							url, preContent);
					String refer = "http://mixer.video.iqiyi.com/jp/recommend/videos?albumId="
							+ id
							+ "&channelId=2&area=panda&size=7&type=video&pru=2145856022&callback=window.Q.__callbacks__.cbp5zfwu";
					String referContent = dongmanFunction.visitURL(refer);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					dongmanFunction.Save(allcontent, refer, "reference", url,
							referContent);

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

					String IqiyiRow = "Iqiyi TvInfo" + id + "#" + time;
					if(allcontent!=null && !allcontent.equals("")) {
						String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
						String line2=allcontent.toString();
						CrawlerThread.saveData(line1, line2);
					}
				}
				allcontent = null;
			} // if
			else { // 完全按照电影或者特殊情况的去处理。
				// StringBuffer sourcecode = new StringBuffer();
				int idstart = movieinfocontent.indexOf("albumId");
				if (idstart == -1) // 按照特殊情况处理。
				{
					int idst = movieinfocontent.indexOf("\"albumId\":\"");
					if (idst >= 0) {
						String idcont = movieinfocontent.substring(idst);
						int idend = idcont.indexOf("\",");
						String id = idcont.substring(idst + 11, idend);

						int tvst = movieinfocontent.indexOf("tvId\":\"");
						String tvcont = movieinfocontent.substring(tvst);
						int tvend = tvcont.indexOf("\",");
						String tv = tvcont.substring(tvst + 7, tvend);
						int qitanst = movieinfocontent.indexOf("qitanId\":");
						int qitanend = movieinfocontent.indexOf(",", qitanst);
						String qitaid = movieinfocontent.substring(qitanst + 9,
								qitanend);

						String refer = "http://mixer.video.iqiyi.com/jp/recommend/videos?albumId="
								+ id
								+ "&channelId=2&area=panda&size=7&type=video&pru=2145856022&callback=window.Q.__callbacks__.cbp5zfwu";
						String referContent = dongmanFunction.visitURL(refer);
						try {
							Thread.sleep(i);
						} catch (Exception e) {
							e.printStackTrace();
						}
						dongmanFunction.Save(allcontent, refer, "reference",
								url, referContent);

					
						String IqiyiRow = "Iqiyi TvInfo" + id + "#" + time;
						if(allcontent!=null && !allcontent.equals("")) {
							String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
							String line2=allcontent.toString();
							CrawlerThread.saveData(line1, line2);
						}
					}
					allcontent = null;
				} else { // 按照电影去处理。
					String idcode = movieinfocontent.substring(idstart);
					int idst = idcode.indexOf("albumId");
					int ided = idcode.indexOf(",");
					String did = idcode.substring(idst, ided);
					String id = did.replaceAll("\\D*", "");

					int tvstart = movieinfocontent.indexOf("tvId");
					String tvcode = movieinfocontent.substring(tvstart);
					int tvst = tvcode.indexOf("tvId");
					int tved = tvcode.indexOf(",");
					String ttv = tvcode.substring(tvst, tved);
					String tv = ttv.replaceAll("\\D*", "");
					int qitanid = movieinfocontent
							.indexOf("data-qitancomment-qitanid=\"");
					String qitanidd = null;
					if (qitanid >= 0) {
						String qitancode = movieinfocontent.substring(qitanid);
						int qitanst = qitancode.indexOf("qitanid=\"");
						int qitanend = qitancode.indexOf("\"", qitanst + 10);
						qitanidd = qitancode.substring(qitanst + 9, qitanend);
					}

					String refer = "http://mixer.video.iqiyi.com/jp/recommend/videos?albumId="
							+ id
							+ "&channelId=2&area=panda&size=7&type=video&pru=2145856022&callback=window.Q.__callbacks__.cbp5zfwu";
					String referContent = dongmanFunction.visitURL(refer);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					dongmanFunction.Save(allcontent, refer, "reference", url,
							referContent);

					
					String precentge = "http://cache.video.qiyi.com/pc/pr/"
							+ id
							+ "/playCountPCMobileCb?callback=playCountPCMobileCb";
					// String
					// precentge="http://cache.video.qiyi.com/pc/pr/"+id+"/playCountPCMobileCb?callback=playCountPCMobileCb";
					String preContent = dongmanFunction.visitURL(precentge);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					dongmanFunction.Save(allcontent, precentge, "precentge",
							url, preContent);
					String guess = "http://mixer.video.iqiyi.com/jp/recommend/videos?referenceId="
							+ tv
							+ "&albumId="
							+ id
							+ "&channelId=1&cookieId=0a0c2b1370b31525ad58b13c07b534f2&withRefer=false&area=bee&size=10&type=video";
					String guessContent = null;
					guessContent = dongmanFunction.visitURL(guess);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					dongmanFunction.Save(allcontent, guess, "Guess", url,
							guessContent);
					String reference = "http://mixer.video.iqiyi.com/jp/recommend/videos?referenceId="
							+ tv
							+ "&albumId="
							+ id
							+ "&channelId=1&cookieId=0a0c2b1370b31525ad58b13c07b534f2&withRefer=true&area=zebra&size=10&type=video";
					String refContent = null;
					refContent = dongmanFunction.visitURL(reference);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					dongmanFunction.Save(allcontent, reference, "Reference",
							url, refContent);

					String sumplaycount = "http://cache.video.qiyi.com/jp/pc/"
							+ id + "/?callback=window.Q.__callbacks__.cbgt6rz7";
					String summcode = null;
					try {
						summcode = dongmanFunction.visitURL(sumplaycount);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					dongmanFunction.Save(allcontent, sumplaycount,
							"Sumplaycount", url, summcode);

					String IqiyiRow = "Iqiyi TvInfo" + id + "#" + time+"@@"+crawltime;
					if(allcontent!=null && !allcontent.equals("")) {
						String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
						String line2=allcontent.toString();
						CrawlerThread.saveData(line1, line2);
					 }
					allcontent = null;
				}
			}
		if	(movieincon == null) {
			jdbc.log("李辉", url+"+iy", 1, "iy", url, "URL访问失败",1);
		}
		}// if jhx;
	 return flag;
	}

	public int  playCrawler(String tempurl, long time, String date,String crawltime,JDBCConnection jdbc) throws IOException {
		StringBuffer allcontent = new StringBuffer();
		String url = null;
	    int flag=-1;
		String tmpurl = tempurl.substring(4);
		String parentid = null;
		if (tmpurl == null || tmpurl.equals(""))
			return flag;
		int urlst = tmpurl.indexOf("^");
		url = tmpurl.substring(0, urlst);
		parentid = tmpurl.substring(urlst + 1);
		String movieinfocontent = "";
		String movieincon = dongmanFunction.visitURL(url);
		if(!movieincon.equals("")) {
			flag=1;
		}
		try {
			Thread.sleep(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		movieinfocontent = "info------------------" + url + movieincon;
		allcontent.append(movieinfocontent);
		int idstart = movieinfocontent.indexOf("albumId");
		if (idstart >= 0) {
			String idcode = movieinfocontent.substring(idstart); // 此处报错。

			int idst = idcode.indexOf("albumId");

			int ided = idcode.indexOf(",");
			String did = idcode.substring(idst, ided);
			String id = did.replaceAll("\\D*", "");
			int tvstart = movieinfocontent.indexOf("tvId");
			String tvcode = movieinfocontent.substring(tvstart);
			int tvst = tvcode.indexOf("tvId");
			int tved = tvcode.indexOf(",");
			String ttv = tvcode.substring(tvst, tved);
			String tv = ttv.replaceAll("\\D*", "");
			int qitanid = movieinfocontent
					.indexOf("data-qitancomment-qitanid=\"");
			int qitaned = movieinfocontent.indexOf("\"", qitanid + 28);
			String qitanidd = movieinfocontent.substring(qitanid + 27, qitaned);

			

			String commurl = "http://api.t.iqiyi.com/qx_api/comment/get_video_comments?sort=hot&tvid="
					+ tv
					+ "&qitan_comment_type=1&page=1&aid="
					+ qitanidd
					+ "&qitanid="
					+ qitanidd
					+ "&categoryid=2&escape=true&need_reply=true&page_size_reply=30&need_total=1&page_size=10&cb=fnsucc&qitancallback=fnsucc";
			String commcontent = null;
			try {
				commcontent = dongmanFunction.visitURL(commurl);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, commurl, "Comment", url,
					commcontent);

			String yurl = "http://api.t.iqiyi.com/qx_api/comment/review/get_review_list?aid="
					+ qitanidd
					+ "&categoryid=2&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=3&page_size=5&page_size_reply=30&qitan_comment_type=1&qitancallback=fnsucc&qitanid="
					+ qitanidd + "&sort=hot&tvid=" + tv;
			String ycontent = null;
			try {
				ycontent = dongmanFunction.visitURL(yurl);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, yurl, "Ycomment", url, ycontent);

			String reference = "http://mixer.video.iqiyi.com/jp/recommend/videos?referenceId="
					+ tv
					+ "&albumId="
					+ id
					+ "&channelId=2&cookieId=c15080ccea46c39cb733eec29be0e154&withRefer=false&area=bee&size=12&type=video&pru=2145856022&callback=window.Q.__callbacks__.cbujithd";
			String refContent = null;
			refContent = dongmanFunction.visitURL(reference);
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, reference, "reference", url,
					refContent);

			String updown = "http://up.video.iqiyi.com/ugc-updown/quud.do?type=2&dataid="
					+ tv
					+ "&userid=&flashuid=0a0c2b1370b31525ad58b13c07b534f2&callback=window.Q.__callbacks__.cbhxrsud";
			String upContent = dongmanFunction.visitURL(updown);
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, updown, "upanddown", url,
					upContent);
			String IqiyiRow = "Iqiyi TvItem" + id + "#" + parentid + "@"+time;
			if(allcontent!=null && !allcontent.equals("")) {
				String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
				String line2=allcontent.toString();
				CrawlerThread.saveData(line1, line2);
			}
			if	(movieincon.equals("")) {
				jdbc.log("李辉", url+"+iy", 1, "iy", url, "URL访问失败",1);
			}
		}
		allcontent = null;
		return flag;
	}
}
