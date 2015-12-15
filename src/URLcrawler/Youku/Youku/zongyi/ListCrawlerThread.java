package URLcrawler.Youku.Youku.zongyi;

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
import Utils.JDBCConnection;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;
import jxHan.Crawler.WebSite.Base.GlobalData;

public class ListCrawlerThread implements Runnable {
	public long time;
	int step;
	public String date;
	public static ArrayList<String> urlList; // 视频列表页面URL集合
	public static HashSet<String> infoplayList = new HashSet<String>();// 数据库中所有infoplayURL集合
	public static HashSet<String> viewyoukuList = new HashSet<String>();// 数据库中所有viewyoukuURL集合
	public static ArrayList<URLcrawler.Youku.Youku.zongyi.ListCrawlerThread> pool;
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
				String line = "youku zongyi youku " + youkuJujiAnalysis + " "
						+ infoId;
				synchronized (viewyoukuList) {
					if (viewyoukuList.add(line)) {
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
				String playLink = "http://www.youku.com/show_episode/" + subURL;
				crawlerEpisode(playLink, infoId);
			}
		
		HashSet<String> yugaohrefs = new HashSet<String>();
		String yugaoHref = "http://www.youku.com/show_around_type_2_title_%E9%A2%84%E5%91%8A%E7%89%87_"
				+ subURL
				+ "?dt=json&__rt=1&__ro=reload_around_type_2_title_%E9%A2%84%E5%91%8A%E7%89%87";
		String yugaoContent = visitURL(yugaoHref);
		try {
			Thread.sleep(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(yugaoContent != null && !yugaoContent.equals("")) {
				String[] yugaopianhref = Filter(yugaoContent, "a", null,
						null, "href", "self", 0);
				HashSet<String> urls = new HashSet<String>();
				if (yugaopianhref != null) {
					for (int i = 0; i < yugaopianhref.length; i++) {
						if (yugaopianhref[i].indexOf("id_") == -1)
							continue;
						if (!yugaohrefs.add(yugaopianhref[i]))
							continue;
						if (urls.add(yugaopianhref[i])) {
							String lineYugao = "youku zongyi play "
									+ yugaopianhref[i] + " " + infoId;
							synchronized (infoplayList) {
								if (infoplayList.add(lineYugao)) {
									StringBuffer infosb = new StringBuffer(
											yugaopianhref[i]);
									String rowKey = infosb
											.reverse()
											.toString()
											.substring(
													infosb.indexOf(".") + 1);
									String[] inforows = { rowKey };
									String[] infocolfams = { "C" };
									String[] infoquals = { "url" };
									String[] infovalues = { lineYugao };
									try {
										 hbase.putRows("infoplayall",
										 inforows, infocolfams, infoquals,
										 infovalues);
									} catch (Exception e) {
										e.printStackTrace();
									}
									inforows = null;
									infocolfams = null;
									lineYugao = null;
									infoquals = null;
									infovalues = null;
									
									String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"yk") + "','" + yugaopianhref[i] + "','yk')";
									conn.update(sql1);
								}
							}
						}

					}
				}
			}
		
	////花絮
					String huaxuHref = "http://www.youku.com/show_around_type_3_title_花絮_"
							+ subURL
							+ "?dt=json&__rt=1&__ro=reload_around_type_3_title_花絮";
					String huaxuContent = visitURL(huaxuHref);
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (huaxuContent != null && !huaxuContent.equals("")) {
						String[] huaxuhref = Filter(huaxuContent, "a", null,
								null, "href", "self", 0);
						HashSet<String> urls = new HashSet<String>();
						if (huaxuhref != null) {
							for (int i = 0; i < huaxuhref.length; i++) {
								if(i == 4)
									break;
								if (huaxuhref[i].indexOf("id_") == -1)
									continue;
								if (!yugaohrefs.add(huaxuhref[i]))
									continue;
								if (urls.add(huaxuhref[i])) {
									String lineYugao = "youku zongyi play "
											+ huaxuhref[i] + " " + infoId;
									synchronized (infoplayList) {
										if (infoplayList.add(lineYugao)) {
											StringBuffer infosb = new StringBuffer(
													huaxuhref[i]);
											String rowKey = infosb
													.reverse()
													.toString()
													.substring(
															infosb.indexOf(".") + 1);
											String[] inforows = { rowKey };
											String[] infocolfams = { "C" };
											String[] infoquals = { "url" };
											String[] infovalues = { lineYugao };
											try {
												 hbase.putRows("infoplayall",
												 inforows, infocolfams, infoquals,
												 infovalues);
											} catch (Exception e) {
												e.printStackTrace();
											}
											inforows = null;
											infocolfams = null;
											lineYugao = null;
											infoquals = null;
											infovalues = null;
											
											String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"+yk") + "','" + huaxuhref[i] + "','yk')";
											conn.update(sql1); //播放页rowkey
										}
									}
								}

							}
						}
					}
					/////////////huaxu  end
		
		
		String id = subURL.substring(3, subURL.indexOf(".html"));
		int page = 1;
		while (true) {
			String moreYugao = "http://www.youku.com/show_around_type_2_title_%E9%A2%84%E5%91%8A%E7%89%87.html?id="
					+ id
					+ "&page="
					+ page
					+ "&dt=json&__rt=1&__ro=around_type_2_title_%E9%A2%84%E5%91%8A%E7%89%87";
			page++;
			String moreContent = visitURL(moreYugao);
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (moreContent == null || moreContent.equals(""))
				break;
			String[] morenhref = Filter(moreContent, "a", null, null,
					"href", "self", 0);
			HashSet<String> moreurls = new HashSet<String>();
			if (morenhref == null)
				break;
			for (int i = 0; i < morenhref.length; i++) {
				if (morenhref[i].indexOf("id_") == -1)
					continue;
				if (moreurls.add(morenhref[i])) {
					if (!yugaohrefs.add(morenhref[i]))
						break;
					String lineYugao = "youku zongyi play "
							+ morenhref[i] + " " + infoId;
					synchronized (infoplayList) {
						if (infoplayList.add(lineYugao)) {
							StringBuffer infosb = new StringBuffer(
									morenhref[i]);
							String rowKey = infosb.reverse().toString()
									.substring(infosb.indexOf(".") + 1);
							String[] inforows = { rowKey };
							String[] infocolfams = { "C" };
							String[] infoquals = { "url" };
							String[] infovalues = { lineYugao };
							try {
								 hbase.putRows("infoplayall",
								 inforows, infocolfams, infoquals,
							    infovalues);
							} catch (Exception e) {
								e.printStackTrace();
							}
							inforows = null;
							infocolfams = null;
							lineYugao = null;
							infoquals = null;
							infovalues = null;
							
							String sql1 = "insert into urls" +dateText + "(rowkey,url,website) values ('" + (rowKey+"yk") + "','" + morenhref[i] + "','yk')";
							conn.update(sql1);
						}
					}
				}
			}
		}
		
	}

	public String crawlerEpisode(String playLink, String infoId)
			throws Exception {
		String playALLlink = null;
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
			if(count == 2)
				return null;
		}
		String[] datas = Filter(playALLlink, "a", null, null, "href", "self", 0);
		if (datas != null && datas.length > 0) {
			for (int i = 0; i < datas.length; i += 2) {
				if (datas[i] == null || datas[i].equals("")
						|| datas[i].indexOf("id_") == -1)
					continue;
					synchronized (infoplayList) {
						if (!infoplayList.add(datas[i])) {
							return null;
						}
					}
					String line = "youku zongyi play "
							+ datas[i] + " " + infoId;
							StringBuffer infosb = new StringBuffer(
									datas[i]);
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
							
							/*String viewAction = "http://index.youku.com/vr_keyword/id_http://v.youku.com/v_show/"
						+ datas[i].substring(datas[i].indexOf("id_"));
							String lineview = "youku zongyi view " + viewAction + " "
						+ datas[i] + " " + infoId;
				//////////////
				synchronized (viewyoukuList) {
					if (viewyoukuList.add(lineview)) {
						StringBuffer viewinfosb = new StringBuffer(
								viewAction);
						String viewrowKey = viewinfosb.reverse().toString()
								.substring(viewinfosb.indexOf(".") + 1);
						String[] viewinforows = { viewrowKey };
						String[] viewinfocolfams = { "C" };
						String[] viewinfoquals = { "url" };
						String[] viewinfovalues = { lineview };
						try {
							  hbase.putRows("viewyouku", viewinforows, viewinfocolfams,
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
				String line = "youku zongyi info " + datas[i];
				synchronized (infoplayList) {
					if (infoplayList.add(line)) {
						StringBuffer infosb = new StringBuffer(
								datas[i]);
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
		//////////////////////
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
					synchronized(pool) {
						pool.remove(this);
						try {
							hbase.commitPuts();
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
			if (count == 5) {
				ExceptionHandler.log(href + " noContent", null);
			
				break;
			}
		}
		return content;
	}

	public static String[] Filter(String content, String tag, String key,
			String value, String parsertarget, String position, int index)  throws Exception{
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
		} catch (Exception e1) {
			ExceptionHandler.log("filter paser exception", e1);
		}
		return datas;
	}

	public static String getData(String extract_href, String parsertarget)  throws Exception{
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
