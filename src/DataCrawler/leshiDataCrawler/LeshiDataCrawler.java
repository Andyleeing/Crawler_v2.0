package DataCrawler.leshiDataCrawler;

import Utils.JDBCConnection;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import DataCrawler.CrawlerThread;

public class LeshiDataCrawler {
	public long time;
	public JDBCConnection jdbc;

	public LeshiDataCrawler(long time, JDBCConnection jdbc) {
		this.time = time;
		this.jdbc = jdbc;
	}

	public int crawler(String url) {
		url = url.substring(6);
		StringBuffer line1 = new StringBuffer();
		StringBuffer line2 = new StringBuffer();

		if (url == null || url.equals(""))
			return -1;
		int vidBeg = url.indexOf("/vplay/");
		if (vidBeg > 0) {
			String type = "";
			int typeEnd = url.indexOf("/moc");
			int typeBegin = url.lastIndexOf("/", typeEnd - 1);
			if (typeEnd < 0 || typeBegin < 0)
				type = "movie";
			else
				type = url.substring(typeBegin + 1, typeEnd);
			line1.append("leshi ");
			line1.append("@type:");
			line1.append(type);
			line1.append(" @date:");
			line1.append(time);
			line1.append("dateend");
			int flag = 3;

			if (url.indexOf("\r\n") > 0)
				url = url.substring(0, url.indexOf("\r\n"));

			String vid = url.substring(vidBeg + 7, url.indexOf(".html"));

			String movieinfoContent = "";
			String pid = "";
			int pidBeg;
			int pidEnd;
			int begin;
			if (type.equals("movie")) {// 电影url中没有info页链接倒置
				movieinfoContent = visitURL(url);
				if (movieinfoContent.length() < 10) {
					jdbc.log("徐萌", pid, 1, "ls", url, movieinfoContent, 1);
					return -1;
				}
				begin = movieinfoContent.indexOf("video");
				pidBeg = movieinfoContent.indexOf("pid:", begin);
				pidEnd = movieinfoContent.indexOf(",", pidBeg);
				if (pidBeg < pidEnd) {
					pid = movieinfoContent.substring(pidBeg + 4, pidEnd);
					if (pid.length() > 40) {
						return -1;
					}
					String sql = "select count(*) as count from videoinfo where inforowkey='"
							+ pid + "' and playrowkey='" + vid + "';";
					if (jdbc.executeQueryCount(sql) > 0) {
						flag = 1;// 表里有该视频info信息
					} else
						flag = 2;// 表里没有视频info信息，但是获取pid时已经有了原网页代码
				}
			} else {
				pidBeg = url.indexOf(".html");
				pidEnd = url.indexOf("/", pidBeg);
				pid = url.substring(pidBeg + 6, pidEnd);
				pid = new StringBuilder(pid).reverse().toString();
				String sql = "select count(*) as count from videoinfo where inforowkey='"
						+ pid + "' and playrowkey='" + vid + "';";
				if (jdbc.executeQueryCount(sql) > 0) {
					flag = 1;// 表里有该视频info信息
				} else
					flag = 3;// 表里没有需要访问url
			}
			if (pid.equals("0")) {
				return 1;
			}
			if (vid != "" && pid != "") {
				line1.append(" @rowkey:");
				line1.append(pid);

				if (flag == 2 || flag == 3) {
					if (flag == 3) {
						url = url.substring(0, url.indexOf(" "));
						movieinfoContent = visitURL(url);
						if (movieinfoContent.length() < 10) {
							jdbc.log("徐萌", pid, 1, "ls", url, movieinfoContent,
									1);
							return -1;
						}
					}
					line2.append("play");
					line2.append(" ");
					line2.append(url);
					line2.append(" ");
					line2.append(movieinfoContent);
					line2.append(" ");
				}
				String infourl = "http://stat.letv.com/vplay/queryMmsTotalPCount?callback=jQuery&pid="
						+ pid + "&cid=1&vid=" + vid;
				String infoContent = visitURL(infourl);
				if (infoContent.length() < 10) {
					jdbc.log("徐萌", pid, 1, "ls", url, infoContent, 1);
					return -1;
				}
				line2.append("info");
				line2.append(" ");
				line2.append(url);
				line2.append(" ");
				line2.append(infourl);
				line2.append(" ");
				line2.append(infoContent);

				String guessyoulikeurl = "http://rec.letv.com/pcw?jsonp=jQuery&pid="
						+ pid + "&vid=" + vid + "&num=12";
				String guessyoulikeContent = visitURL(guessyoulikeurl);
				if (guessyoulikeContent.length() < 10) {
					jdbc.log("徐萌", pid, 1, "ls", url, guessyoulikeContent, 1);
					return -1;
				}

				line2.append("guessyoulike");
				line2.append(" ");
				line2.append(url);
				line2.append(" ");
				line2.append(guessyoulikeurl);
				line2.append(" ");
				line2.append(guessyoulikeContent);

				CrawlerThread.saveData(line1.toString(), line2.toString());
			}
		}
		return 1;
	}

	public static String visitURL(String href) {
		String content = null;
		int count = 0;
		while (true) {
			content = ConnectioinFuction.readURL(href);
			if (content != null && !content.equals(""))
				break;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
			if (count == 10) {
				break;
			}
		}
		return content;
	}
}
