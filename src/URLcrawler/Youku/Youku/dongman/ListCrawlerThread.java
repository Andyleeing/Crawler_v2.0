package URLcrawler.Youku.Youku.dongman;

import hbase.HBaseCRUD;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import Utils.JDBCConnection;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;
import jxHan.Crawler.WebSite.Base.GlobalData;

public class ListCrawlerThread implements Runnable {
	public long time;
	int step;
	public String date;
	ArrayList<String> test = new ArrayList<String>();
	public static ArrayList<String> urlList; // 视频列表页面URL集合
	public static HashSet<String> infoplayList = new HashSet<String>();// 数据库中所有infoplayURL集合
	public static HashSet<String> viewyouku1List = new HashSet<String>();// 数据库中所有viewyouku1URL集合
	public static ArrayList<URLcrawler.Youku.Youku.dongman.ListCrawlerThread> pool;
	public int i = 50;
	public HBaseCRUD hbase = new HBaseCRUD();
	public JDBCConnection conn = new JDBCConnection();
	public String dateText;
	public ListCrawlerThread(long time, String date, int step) {
		this.time = time;
		this.date = date;
		this.step = step;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		dateText = sdf.format(new Date(time));
	}

	public void URLCrawler(String url) throws Exception {
		StringBuffer sb = new StringBuffer(url);
		String subURL = url.substring(url.indexOf("id_z"));
		String infoId = sb.reverse().toString().substring(sb.indexOf(".") + 1);
		String movieinfoContent = "";
		if (url.indexOf("show_page") > 0) {
			movieinfoContent = visitURL(url);
			String[] jujiAnalysis = Filter(movieinfoContent, "span", "class",
					"vr", "href", "child", 3);
			if (jujiAnalysis != null && jujiAnalysis.length > 0) {
				String youkuJujiAnalysis = jujiAnalysis[0];
				String line = "youku dongman youku " + youkuJujiAnalysis + " "
						+ infoId;
				synchronized (viewyouku1List) {
					if (viewyouku1List.add(line)) {
						StringBuffer infosb = new StringBuffer(
								youkuJujiAnalysis);
						String rowKey = infosb.reverse().toString()
								.substring(infosb.indexOf(".") + 1);
						String[] inforows = { rowKey };
						String[] infocolfams = { "C" };
						String[] infoquals = { "url" };
						String[] infovalues = { line };
						try {
							 hbase.putRows("viewyouku", inforows, infocolfams,
							 infoquals,
							 infovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
						inforows = null;
						infocolfams = null;
						line = null;
						infoquals = null;
						infovalues = null;
						
						String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"yk") + "','" + youkuJujiAnalysis + "','yk')";
						conn.update(sql1);
					}
				}
			}
			else 
				//add by hanjiaxing 2015.06.30
				conn.log("韩江雪", "", 1, "yk", url, "URL地址异常", 3);
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//////////////
			String playLink = "http://www.youku.com/show_episode/" + subURL;
			String str = "?dt=json&divid=reload_";
			String playALLlink = crawlerEpisode(playLink+str+1, infoId);
			int i = 20;
			while (true) {
				//System.out.println(test.size());
				String link = playLink;
				if (playALLlink != null && !playALLlink.equals("")) {
					link += str + (i+1);
					i += 20;
					playALLlink = crawlerEpisode(link, infoId);
				} else {
					break;
				}
			}
			// /////////////////////////////////////
			String[] yugaopianhref = Filter(movieinfoContent, "a", "class",
					"btnShow btnplaytrailer", "href", "self", 0);
			if (yugaopianhref != null && yugaopianhref.length > 0) {
				String line = "youku dongman play " + yugaopianhref[0] + " "
						+ infoId;
				synchronized (infoplayList) {
					if (infoplayList.add(line)) {
						StringBuffer infosb = new StringBuffer(yugaopianhref[0]);
						String rowKey = infosb.reverse().toString()
								.substring(infosb.indexOf(".") + 1);
						String[] inforows = { rowKey };
						String[] infocolfams = { "C" };
						String[] infoquals = { "url" };
						String[] infovalues = { line };
						try {
								hbase.putRows("infoplayall", inforows, infocolfams,
									infoquals, infovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
						inforows = null;
						infocolfams = null;
						line = null;
						infoquals = null;
						infovalues = null;
						
						String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"yk") + "','" + yugaopianhref[0] + "','yk')";
						conn.update(sql1);
					}
				}
				
				
				/*String viewAction = "http://index.youku.com/vr_keyword/id_http://v.youku.com/v_show/"
						+ yugaopianhref[0].substring(yugaopianhref[0]
								.indexOf("id_"));
				String lineview = "youku movie view " + viewAction + " "
						+ yugaopianhref[0] + " " + infoId;
				synchronized (viewyouku1List) {
					if (viewyouku1List.add(lineview)) {
						StringBuffer infosb = new StringBuffer(viewAction);
						String rowKey = infosb.reverse().toString()
								.substring(infosb.indexOf(".") + 1);
						String[] inforows = { rowKey };
						String[] infocolfams = { "C" };
						String[] infoquals = { "url" };
						String[] infovalues = { lineview };
						try {
							hbase.putRows("viewyouku", inforows, infocolfams,
									infoquals, infovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}*/
			}
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public String crawlerEpisode(String playLink, String infoId)
			throws Exception {
		String playALLlink;
		int count = 0;
		while(true) {
			playALLlink = visitURL(playLink);
			if(playALLlink!= null && playALLlink.indexOf("暂无内容") >= 0) {
				count++;
			}
			else 
				break;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(count == 5) 
				return null;
		}
		String[] datas = Filter(playALLlink, "a", null, null, "href", "self", 0);
		if (datas != null && datas.length > 0) {
			for (int i = 0; i < datas.length; i++) {
				if (datas[i] == null || datas[i].equals("")
						|| datas[i].indexOf("id_") == -1)
					continue;
				synchronized (infoplayList) {
					if (!infoplayList.add(datas[i])) {
						return null;
					}
				}
				String line = "youku dongman play " + datas[i] + " " + infoId;
				StringBuffer infosb = new StringBuffer(datas[i]);
				String rowKey = infosb.reverse().toString()
						.substring(infosb.indexOf(".") + 1);
				String[] inforows = { rowKey };
				String[] infocolfams = { "C" };
				String[] infoquals = { "url" };
				String[] infovalues = { line };
				try {
					 hbase.putRows("infoplayall",
					 inforows, infocolfams, infoquals,
					 infovalues);
					//test.add(line);
				} catch (Exception e) {
					e.printStackTrace();
				}
				inforows = null;
				infocolfams = null;
				line = null;
				infoquals = null;
				infovalues = null;
				
				String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"yk") + "','" + datas[i] + "','yk')";
				conn.update(sql1);
				
				/*String viewAction = "http://index.youku.com/vr_keyword/id_http://v.youku.com/v_show/"
						+ datas[i].substring(datas[i].indexOf("id_"));
				String lineview = "youku dongman view " + viewAction + " "
						+ datas[i] + " " + infoId;
				// ////////////
				synchronized (viewyouku1List) {
					if (viewyouku1List.add(lineview)) {
						StringBuffer viewinfosb = new StringBuffer(viewAction);
						String viewrowKey = viewinfosb.reverse().toString()
								.substring(viewinfosb.indexOf(".") + 1);
						String[] viewinforows = { viewrowKey };
						String[] viewinfocolfams = { "C" };
						String[] viewinfoquals = { "url" };
						String[] viewinfovalues = { lineview };
						try {
							 hbase.putRows("viewyouku", viewinforows,
							 viewinfocolfams,
							 viewinfoquals,
						    viewinfovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
						viewinforows = null;
						viewinfocolfams = null;
						lineview = null;
						viewinfoquals = null;
						viewinfovalues = null;
					}
				}*/
			}
		} else
			playALLlink = null;
		try {
			Thread.sleep(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return playALLlink;
	}

	public void ListCrawler(String url) throws Exception {
		String listContent = "";
		listContent = visitURL(url);
		String[] datas = Filter(listContent, "div", "class", "p-link", "href",
				"child", 1);
		if (datas != null && datas.length > 0) {
			for (int i = 0; i < datas.length; i++) {
				URLCrawler(datas[i]);
				String line = "youku dongman info " + datas[i];
				synchronized (infoplayList) {
					if (infoplayList.add(line)) {
						StringBuffer infosb = new StringBuffer(datas[i]);
						String rowKey = infosb.reverse().toString()
								.substring(infosb.indexOf(".") + 1);
						String[] inforows = { rowKey };
						String[] infocolfams = { "C" };
						String[] infoquals = { "url" };
						String[] infovalues = { line };
						try {
							 hbase.putRows("infoplayall",
							 inforows, infocolfams, infoquals,
							 infovalues);
						} catch (Exception e) {
							e.printStackTrace();
						}
						inforows = null;
						infocolfams = null;
						line = null;
						infoquals = null;
						infovalues = null;
						
						String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"yk") + "','" + datas[i] + "','yk')";
						conn.update(sql1);
					}
				}
			}
		}
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// ////////////////////
	}

	@Override
	public void run() {
		while (true) {
			String url = "";
			synchronized (urlList) {
				if (urlList != null && urlList.size() > 0) {
					url = urlList.get(0);
					urlList.remove(0);
				} else if (urlList == null || urlList.size() == 0) {
					synchronized (pool) {
						pool.remove(this);
						try {
							hbase.commitPuts();
							conn.closeConn();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					break;
				}
			}
			if (step == 1) {
				try {
					ListCrawler(url);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String visitURL(String href) {
		String content = null;
		int count = 0;
		while (true) {
			content = ConnectioinFuction.readURL(href);
			if (content != null && !content.equals(""))
				break;
			try {
				Thread.sleep(i);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
			if (count == 2) {
				ExceptionHandler.log(href + " noContent", null);
				break;
			}
		}
		return content;
	}

	public static String[] Filter(String content, String tag, String key,
			String value, String parsertarget, String position, int index) throws Exception {
		String[] datas = null;
		NodeFilter filter = null;
		if (key == null || value == null)
			filter = new TagNameFilter(tag);
		else
			filter = new AndFilter(new TagNameFilter(tag),
					new HasAttributeFilter(key, value));
		try {
			Parser parser = new Parser(content);
			NodeList movielist = parser.parse(filter);
			if (movielist.size() > 0)
				datas = new String[movielist.size()];
			for (int i = 0; i < movielist.size(); i++) {
				Node movie = movielist.elementAt(i);
				String extract_href = "";
				if (position != null && position.equals("child"))
					extract_href = movie.getChildren().elementAt(index)
							.getText();
				else if (position != null && position.equals("self"))
					extract_href = movie.getText();
				if (GlobalData.category != null
						&& GlobalData.category.equals("star"))
					datas[i] = getDataForYoukuStar(extract_href, parsertarget);
				else {
					datas[i] = getData(extract_href, parsertarget);
				}
			}
		} catch (ParserException e1) {
			ExceptionHandler.log("filter paser exception", e1);
		}
		return datas;
	}

	public static String getData(String extract_href, String parsertarget) throws Exception{
		String href = "";
		int indexofhref = extract_href.indexOf(parsertarget);
		if (indexofhref < 0)
			return href;
		href = extract_href.substring(indexofhref);
		indexofhref = href.indexOf("\"") + 1;
		href = href.substring(indexofhref, href.indexOf("\"", indexofhref));
		return href;
	}

	public static String getDataForYoukuStar(String extract_href,
			String parsertarget)  throws Exception{
		String href = "";
		if (extract_href.indexOf("star_page") > 0) {
			int indexofhref = extract_href.indexOf(parsertarget);
			href = extract_href.substring(indexofhref);
			indexofhref = href.indexOf("\"") + 1;
			href = href.substring(indexofhref, href.indexOf("\"", indexofhref));
		}
		return href;
	}
}
