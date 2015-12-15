package URLcrawler.Xunlei;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;

public class CrawlerURLs {
	private static int threadCount = 10;
	public static ArrayList<String> threadurls = new ArrayList<String>();
	public JDBCConnection jdbconn = new JDBCConnection();
	public void crawler() {
		try {
			Date date = new Date();
			updataMovieUrl(date,
						"http://movie.kankan.com/type,order/movie,hits/");

			updataTVUrl(date, "http://movie.kankan.com/type/tv/");
			updataTVPlayUrl(date, "http://movie.kankan.com/type/teleplay/");
			updataAnimationUrl(date,
						"http://movie.kankan.com/type,order,status/anime,hits,zp/");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 多线程抓取迅雷看看电影URL列表
	 */
	public  void updataMovieUrl(Date d, String strUrl) {
		try {
			ArrayList<MovieUrlList> pool = new ArrayList<MovieUrlList>();
			ArrayList<String> list = getURLsFromMoviePage(strUrl);
			if (list == null || list.size() <= 0) {
				System.out.println("movie urls error!");
				return;
			}
			MovieUrlList.urlList = list;
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			MovieUrlList.pool = pool;
			for (int i = 0; i < threadCount; i++) {
				MovieUrlList urls = new MovieUrlList(d.getTime(), df.format(d));

				pool.add(urls);
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				new Thread(urls).start();
			}
			pool = null;
			int count = 0;
			while (true) {
				synchronized (MovieUrlList.pool) {
					if (MovieUrlList.pool.size() <= 0) {
						MovieUrlList.pool = null;
						MovieUrlList.map.clear();
						break;
					}
				}
				count++;
				if (count > 5)
					break;
				Thread.sleep(120000);

			}
			System.out.println("movie ok");
		}// try
		catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			// System.out.println("crawler" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 多线程抓取迅雷看看动漫URL列表
	 */
	public  void updataAnimationUrl(Date d, String strUrl) {
		try {
			ArrayList<AnimationUrlList> pool = new ArrayList<AnimationUrlList>();
			ArrayList<String> list = getURLsFromAnimationPage(strUrl);
			if (list == null || list.size() <= 0) {
				System.out.println("animation urls error!");
				return;
			}
			AnimationUrlList.urlList = list;
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			AnimationUrlList.pool = pool;
			for (int i = 0; i < threadCount; i++) {
				AnimationUrlList urls = new AnimationUrlList(d.getTime(),
						df.format(d));

				pool.add(urls);
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				new Thread(urls).start();
			}
			pool = null;
			int count = 0;
			while (true) {
				synchronized (AnimationUrlList.pool) {
					System.out.println(AnimationUrlList.pool.size());
					if (AnimationUrlList.pool.size() <= 0) {
						AnimationUrlList.pool = null;
						AnimationUrlList.map.clear();
						break;
					}
				}
				count++;
				if (count > 5)
					break;
				Thread.sleep(120000);
			}
			System.out.println("animation ok");
		}// try
		catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			// System.out.println("crawler" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 多线程抓取迅雷看看综艺URL列表
	 */
	public  void updataTVUrl(Date d, String strUrl) {
		try {
			ArrayList<TVUrlList> pool = new ArrayList<TVUrlList>();
			ArrayList<String> list = getURLsFromTVPage(strUrl);
			if (list == null || list.size() <= 0) {
				System.out.println("tv urls error!");
				return;
			}
			TVUrlList.urlList = list;

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			TVUrlList.pool = pool;
			for (int i = 0; i < threadCount; i++) {
				TVUrlList urls = new TVUrlList(d.getTime(), df.format(d));

				pool.add(urls);
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				new Thread(urls).start();
			}
			pool = null;
			int count = 0;
			while (true) {
				synchronized (TVUrlList.pool) {
					if (TVUrlList.pool.size() <= 0) {
						TVUrlList.pool = null;
						TVUrlList.map.clear();
						break;
					}
				}

				count++;
				if (count > 5)
					break;

				Thread.sleep(120000);
			}
			System.out.println("tv ok");
		} catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			// System.out.println("crawler" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 多线程抓取迅雷看看电视剧URL列表
	 */
	public  void updataTVPlayUrl(Date d, String strUrl) {
		try {
			ArrayList<TVPlayUrlList> pool = new ArrayList<TVPlayUrlList>();
			ArrayList<String> list = getURLsFromTVPlayPage(strUrl);

			if (list == null || list.size() <= 0) {
				System.out.println("tvplay urls error!");
				return;
			}
			TVPlayUrlList.urlList = list;

			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			TVPlayUrlList.pool = pool;
			for (int i = 0; i < threadCount; i++) {
				TVPlayUrlList urls = new TVPlayUrlList(d.getTime(),
						df.format(d));

				pool.add(urls);
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				new Thread(urls).start();
			}
			pool = null;
			int count = 0;
			while (true) {
				synchronized (TVPlayUrlList.pool) {
					if (TVPlayUrlList.pool.size() <= 0) {
						TVPlayUrlList.pool = null;
						TVPlayUrlList.map.clear();
						break;
					}
				}

				count++;
				if (count > 5)
					break;

				Thread.sleep(120000);
			}
			System.out.println("tvplay ok");
		} catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			// System.out.println("crawler" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 获取迅雷看看电影第一级URL列表
	 */
	public  ArrayList<String> getURLsFromMoviePage(String strUrl) {
		ArrayList<String> urls = new ArrayList<String>();
		DocumetContent docContent = new DocumetContent(jdbconn);
		Document doc = docContent.getDocument(strUrl);
		int pageCount = 0;
		int dataCount = 0;
		if (doc != null) {
			Element dataNum1 = doc.getElementsByAttributeValue("class",
					"s_o_result").first();
			String dataNumTemp = dataNum1.text();
			Pattern pattern = Pattern.compile("[^0-9]");

			Matcher matcher = pattern.matcher(dataNumTemp);// 去除字符串中非数字的字符
			String dataNum = matcher.replaceAll("");

			dataCount = Integer.parseInt(dataNum);
			pageCount = dataCount / 30;
			if (dataCount % 30 != 0) {
				pageCount += 1;
			}
		}

		Elements allLink = null;
		Element contentPart = null;

		int vipIndex = -2;

		if (dataCount > 0) {
			contentPart = doc.getElementById("movie_list");
			allLink = contentPart.getElementsByClass("pic").select("a[href]");// allLink中存储每页中各个电影的链接。
			for (Element link : allLink) {
				if (link != null) {
					String temp = link.attr("abs:href");
					vipIndex = temp.indexOf("vip");
					if (vipIndex == 7) {
						// vip.kankan的URL
					} else {
						urls.add(temp);
					}

				}

			}
		}
		if (pageCount > 1) {
			int currentPage = 2;
			String otherUrl = null;
			while (currentPage <= pageCount) {
				Element otherPage = doc.getElementsByClass("list-pager-v2")
						.select("a[href]").first();
				otherUrl = otherPage.absUrl("href");
				if (otherUrl != null) {
					int index = otherUrl.indexOf("hits");
					if (index < 0)
						break;
					String HeadUrl = otherUrl.substring(0, index);
					String realUrl = HeadUrl + "hits/page" + currentPage + "/";
					Document doctemp = docContent.getDocument(realUrl);
					if (doctemp != null) {
						contentPart = doctemp.getElementById("movie_list");
						allLink = contentPart.getElementsByClass("pic").select(
								"a[href]");
						for (Element link : allLink) {
							if (link != null) {
								String temp = link.attr("abs:href");// +"\r\n";
								vipIndex = temp.indexOf("vip");
								if (vipIndex == 7) {
									// vip.kankan的URL
								} else {
									urls.add(temp);
								}
							}// if
						}// for

					} else
						System.out.println("no content" + realUrl);
				}// if otherUrl!=null
				currentPage++;
			}// while
		}// if(pageCount)

		return urls;
	}

	/*
	 * 
	 * 获取迅雷看看电视剧第一级URL列表
	 */
	public  ArrayList<String> getURLsFromTVPlayPage(String strUrl) {
		ArrayList<String> urls = new ArrayList<String>();

		DocumetContent docContent = new DocumetContent(jdbconn);
		Document doc = docContent.getDocument(strUrl);

		int pageCount = 0;
		int dataCount = 0;
		if (doc != null) {
			Element dataNum1 = doc.getElementsByAttributeValue("class",
					"s_o_result").first();
			String dataNumTemp = dataNum1.text();
			Pattern pattern = Pattern.compile("[^0-9]");
			Matcher matcher = pattern.matcher(dataNumTemp);
			String dataNum = matcher.replaceAll("");

			dataCount = Integer.parseInt(dataNum);
			pageCount = dataCount / 30;
			if (dataCount % 30 != 0) {
				pageCount += 1;
			}
		}

		Elements allLink = null;
		Element contentPart = null;

		int vipIndex = -2;

		if (dataCount > 0) {
			contentPart = doc.getElementById("movie_list");
			allLink = contentPart.getElementsByClass("pic").select("a[href]");// allLink中存储每页中各个电影的链接。
			for (Element link : allLink) {
				if (link != null) {
					String temp = link.attr("abs:href");
					vipIndex = temp.indexOf("vip");
					if (vipIndex == 7) {
						// vip.kankan的URL
					} else {
						urls.add(temp);
					}

				}

			}
		}
		if (pageCount > 1) {
			int currentPage = 2;
			String otherUrl = null;
			while (currentPage <= pageCount) {
				Element otherPage = doc.getElementsByClass("list-pager-v2")
						.select("a[href]").first();
				otherUrl = otherPage.absUrl("href");
				if (otherUrl != null) {
					int index = otherUrl.indexOf("teleplay");
					if (index < 0)
						break;
					String HeadUrl = otherUrl.substring(0, index);
					String realUrl = HeadUrl + "teleplay/page" + currentPage
							+ "/";
					Document doctemp = docContent.getDocument(realUrl);
					if (doctemp != null) {
						contentPart = doctemp.getElementById("movie_list");
						allLink = contentPart.getElementsByClass("pic").select(
								"a[href]");
						for (Element link : allLink) {
							if (link != null) {
								String temp = link.attr("abs:href");// +"\r\n";
								vipIndex = temp.indexOf("vip");

								if (vipIndex == 7) {
									// vip.kankan的URL
									//
								} else {
									urls.add(temp);
								}
							}// if
						}// for

					} else
						System.out.println("no content" + realUrl);

				}// if otherUrl!=null
				currentPage++;
			}// while
		}// if(pageCount)

		return urls;
	}

	/*
	 * 
	 * 获取迅雷看看动漫第一级URL列表
	 */
	public  ArrayList<String> getURLsFromAnimationPage(String strUrl) {
		ArrayList<String> urls = new ArrayList<String>();

		DocumetContent docContent = new DocumetContent(jdbconn);
		Document doc = docContent.getDocument(strUrl);

		int pageCount = 0;
		int dataCount = 0;
		Elements allLink = null;
		Element contentPart = null;
		if (doc != null) {
			Element dataNum1 = doc.getElementsByAttributeValue("class",
					"s_o_result").first();
			String dataNumTemp = dataNum1.text();
			Pattern pattern = Pattern.compile("[^0-9]");
			Matcher matcher = pattern.matcher(dataNumTemp);
			String dataNum = matcher.replaceAll("");

			dataCount = Integer.parseInt(dataNum);
			pageCount = dataCount / 30;
			if (dataCount % 30 != 0) {
				pageCount += 1;
			}
		}

		int vipIndex = -2;

		if (dataCount > 0) {
			contentPart = doc.getElementById("movie_list");
			allLink = contentPart.getElementsByClass("pic").select("a[href]");// allLink中存储每页中各个电影的链接。
			for (Element link : allLink) {
				if (link != null) {
					String temp = link.attr("abs:href");
					vipIndex = temp.indexOf("vip");
					if (vipIndex == 7) {
						// vip.kankan的URL
					} else {
						urls.add(temp);
					}

				}

			}
		}
		if (pageCount > 1) {
			int currentPage = 2;
			String otherUrl = null;
			while (currentPage <= pageCount) {
				Element otherPage = doc.getElementsByClass("list-pager-v2")
						.select("a[href]").first();
				otherUrl = otherPage.absUrl("href");
				if (otherUrl != null) {
					int index = otherUrl.indexOf("zp");
					if (index < 0)
						break;
					String HeadUrl = otherUrl.substring(0, index);
					String realUrl = HeadUrl + "zp/page" + currentPage + "/";
					Document doctemp = docContent.getDocument(realUrl);
					if (doctemp != null) {
						contentPart = doctemp.getElementById("movie_list");
						allLink = contentPart.getElementsByClass("pic").select(
								"a[href]");
						for (Element link : allLink) {
							if (link != null) {
								String temp = link.attr("abs:href");// +"\r\n";
								vipIndex = temp.indexOf("vip");

								if (vipIndex == 7) {
									// vip.kankan的URL
									//
								} else {
									urls.add(temp);
								}
							}// if
						}// for

					} else
						System.out.println("no content" + realUrl);

				}// if otherUrl!=null
				currentPage++;
			}// while
		}// if(pageCount)

		return urls;
	}

	public  ArrayList<String> getURLsFromTVPage(String strUrl) {
		ArrayList<String> urls = new ArrayList<String>();

		DocumetContent docContent = new DocumetContent(jdbconn);
		Document doc = docContent.getDocument(strUrl);

		int pageCount = 0;
		int dataCount = 0;
		Elements allLink = null;
		Element contentPart = null;
		if (doc != null) {
			Element dataNum1 = doc.getElementsByAttributeValue("class",
					"s_o_result").first();
			String dataNumTemp = dataNum1.text();
			Pattern pattern = Pattern.compile("[^0-9]");
			Matcher matcher = pattern.matcher(dataNumTemp);
			String dataNum = matcher.replaceAll("");

			dataCount = Integer.parseInt(dataNum);
			pageCount = dataCount / 30;
			if (dataCount % 30 != 0) {
				pageCount += 1;
			}
		}

		int vipIndex = -2;

		if (dataCount > 0) {
			contentPart = doc.getElementById("movie_list");
			allLink = contentPart.getElementsByClass("pic").select("a[href]");// allLink中存储每页中各个电影的链接。
			for (Element link : allLink) {
				if (link != null) {
					String temp = link.attr("abs:href");
					vipIndex = temp.indexOf("vip");
					if (vipIndex == 7) {
						// vip.kankan的URL
					} else {
						urls.add(temp);
					}

				}

			}
		}
		if (pageCount > 1) {
			int currentPage = 2;
			String otherUrl = null;
			while (currentPage <= pageCount) {
				Element otherPage = doc.getElementsByClass("list-pager-v2")
						.select("a[href]").first();
				otherUrl = otherPage.absUrl("href");
				if (otherUrl != null) {
					int index = otherUrl.indexOf("tv");
					if (index < 0)
						break;
					String HeadUrl = otherUrl.substring(0, index);
					String realUrl = HeadUrl + "tv/page" + currentPage + "/";
					Document doctemp = docContent.getDocument(realUrl);
					if (doctemp != null) {
						contentPart = doctemp.getElementById("movie_list");
						allLink = contentPart.getElementsByClass("pic").select(
								"a[href]");
						for (Element link : allLink) {
							if (link != null) {
								String temp = link.attr("abs:href");// +"\r\n";
								vipIndex = temp.indexOf("vip");

								if (vipIndex == 7) {
									// vip.kankan的URL
									//
								} else {
									urls.add(temp);
								}
							}// if
						}// for

					} else
						System.out.println("no content" + realUrl);

				}// if otherUrl!=null
				currentPage++;
			}// while
		}// if(pageCount)

		return urls;
	}


	public static String getMovieId(String strUrl) {
		try {
			int sub1 = strUrl.lastIndexOf('/');

			String movieIdTemp = strUrl.substring(0, sub1);
			int sub2 = movieIdTemp.lastIndexOf('/');
			String movieId = movieIdTemp.substring(sub2 + 1);
			return movieId;
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
