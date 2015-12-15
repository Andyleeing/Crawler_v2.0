package URLcrawler.Tencent;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import hbase.HBaseCRUD;

import org.jsoup.Jsoup;
import org.jsoup.helper.HttpConnection;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import Utils.TextValue;

public class OutputFileZongYi {
	public static FileWriter fw = null;
	public static FileWriter fx = null;// 查看文件保存完毕没有
	public HBaseCRUD hbase;

	public OutputFileZongYi(HBaseCRUD hbase) {
		this.hbase = hbase;
	}

	/*
	 * 思路：通过目录页url，先得到每个栏目最新一期的play页url，然后得到info页，然后从info页得到
	 * vkey，再通过vkey构造节目列表url 作者：石嘉帆 版本：1.0 参数：String strUrl
	 * ：通过URLMaker得到的目录页url传入进来
	 */
	public void tencentOutput(String strUrl, String sqlTableName) {
		try {
			JDBCConnection jdbc = new JDBCConnection();
			Document doc = ceshi.getdoc(strUrl);
			int count = 0;
			while (doc == null) {
				doc = ceshi.getdoc(strUrl);
				if (count >= 5) {
					jdbc.log("石嘉帆", "", 1, "tx", strUrl, "url地址抓取异常", 3);
					return;
				}
				count++;
			}
			Elements allLink = doc.getElementsByClass("mod_pic");
			// 每一个link都是一个节目
			for (Element link : allLink) {
				String temp = link.toString();
				int aPos = temp.indexOf("http:");
				int bPos = temp.indexOf("\" class");
				// everyUrl是某个节目play页的 url
				String everyUrl = temp.substring(aPos, bPos);
				Document zongYiPlay = ceshi.getdoc(everyUrl);
				// 从play页的原网页得到info页,先存入hbase中 格式： url + 0.0 * type:zongyi
				Elements linkLanMu = null;
				try {
					linkLanMu = zongYiPlay.getElementsByClass("link_phase");
				} catch (Exception e) {
					continue;
				}
				String infoUrl = linkLanMu.attr("href");
				int akey = infoUrl.indexOf("detail");
				int bkey = infoUrl.indexOf(".html");
				if (akey >= bkey || akey < 0) {
					continue;
				}
				String key = infoUrl.substring(akey, bkey);
				String value = infoUrl + " + " + "0.0" + " * type:zongyi";
				String tableName = "tencentnew";
				String[] rows = { key };
				String[] colfams = { "C" };
				String[] quals = { "url" };
				String[] values = { "tencent " + value };
				try {
					hbase.putRows(tableName, rows, colfams, quals, values);
					ArrayList<TextValue> values2 = new ArrayList<TextValue>();
					TextValue rowkeytv = new TextValue();
					rowkeytv.text = "rowkey";
					rowkeytv.value = key;
					values2.add(rowkeytv);

					TextValue urltv = new TextValue();
					urltv.text = "url";
					urltv.value = value;
					values2.add(urltv);

					TextValue websitetv = new TextValue();
					websitetv.text = "website";
					websitetv.value = "tx";
					values2.add(websitetv);

					jdbc.insert(values2, sqlTableName);
					values2 = null;
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println(value);

				Document zongYiInfo = ceshi.getdoc(infoUrl);
				String zongYiInfoPage = zongYiInfo.toString();
				int asourceId = zongYiInfoPage.indexOf(" sourceid=\"");
				int bsourceId = zongYiInfoPage.indexOf("\" sourcename=\"",
						asourceId);
				if (asourceId < 0 && bsourceId < 0)
					break;
				String sourceId = null;
				try {
					sourceId = zongYiInfoPage.substring(asourceId + 11,
							bsourceId);
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					jdbc.closeConn();
				} catch (Exception e) {
					e.printStackTrace();
				}
				saveUrl(sourceId, sqlTableName);
			}
		} catch (Exception e) {

		}
	}

	public void saveUrl(String sourceId, String sqlTableName) {
		JDBCConnection jdbc = new JDBCConnection();
		String aAllPlay = "http://s.video.qq.com/loadplaylist?vkey=";
		String bAllPlay = "&vtype=3&otype=json";
		// 另一个字段：&content=\"text/html;%20charset=utf-8\"
		String allPlay = aAllPlay + sourceId + bAllPlay;
		HttpConnection conn = (HttpConnection) Jsoup.connect(allPlay);
		Document doc = new HC().getPage(allPlay);
		int count = 0;
		while (doc == null) {
			System.out.println(count++);
			doc = ceshi.getdoc(allPlay);
			if (count == 5) {
				break;
			}
		}
		String page = doc.toString();
		String date = null;
		String name = null;
		String url = null;
		String cId = null;
		// OutputFileZongYi ofzy = new OutputFileZongYi();
		page = page.replaceAll("&quot;", "");
		page = page.replaceAll("&middot;", "");
		// key 用cid来代替
		while (page.contains("episode_number")) {
			date = findDate(page);
			name = findName(page);
			if (name.contains("de_number:")) {
				name = "";
			}
			url = findUrl(page);
			if (url.length() != 44) {
				// 可能有别的网站的连接，比如iqiyi，就不抓了
				break;
			}
			cId = findCid(page);
			page = cut(page);
			String value = url + "  " + name + " @ " + date + " * type:zongyi";
			// try {
			// fw.write(value + "\r\n");
			// fw.flush();
			// } catch (IOException e) {
			// e.printStackTrace();
			// }
			String[] rows = { cId };
			String[] colfams = { "C" };
			String[] quals = { "url" };
			String[] values = { "tencent " + value };
			try {
				hbase.putRows("tencentnew", rows, colfams, quals, values);
				ArrayList<TextValue> values2 = new ArrayList<TextValue>();
				TextValue rowkeytv = new TextValue();
				rowkeytv.text = "rowkey";
				rowkeytv.value = cId;
				values2.add(rowkeytv);

				TextValue urltv = new TextValue();
				urltv.text = "url";
				urltv.value = value;
				values2.add(urltv);

				TextValue websitetv = new TextValue();
				websitetv.text = "website";
				websitetv.value = "tx";
				values2.add(websitetv);

				jdbc.insert(values2, sqlTableName);
				values2 = null;
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println(value);
		}
		jdbc.closeConn();
	}

	public String findDate(String page) {
		String date = "";
		int begin = page.indexOf("episode_number:");
		int end = page.indexOf(",id:");
		if (begin < end) {
			date = page.substring(begin + 15, end);
		}
		return date;
	}

	public String findName(String page) {
		String name = "";
		int begin = page.indexOf("title:");
		int end = page.indexOf(",type:");
		if (begin < end && begin > 0) {
			name = page.substring(begin + 6, end);
		}
		return name;
	}

	public String findUrl(String page) {
		String url = "";
		int begin = page.indexOf("url:");
		int end = page.indexOf("},{");
		if (end < 0) {
			end = page.indexOf("}]");
		}
		if (begin < end) {
			url = page.substring(begin + 4, end);
			if (url.length() > 50 || url.length() < 3) {
				end = page.indexOf("}]");
				url = page.substring(begin + 4, end);
			}
		}
		return url;
	}

	public String findCid(String page) {
		String cId = "";
		int begin = page.indexOf("id:");
		int end = page.indexOf(",is_new");
		if (begin < end) {
			cId = page.substring(begin + 3, end);
		}
		return cId;
	}

	public String cut(String page) {
		String newPage = "";
		int begin = page.indexOf("},{");
		if (begin < 0)
			return "";
		newPage = page.substring(begin + 3);
		return newPage;
	}

}

class HC {
	public Document getPage(String url) {
		FileWriter fx = null;
		try {
			fx = new FileWriter("src/URLcrawler/Tencent/tencentLog.txt", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		HttpConnection conn = (HttpConnection) Jsoup.connect(url);
		conn.timeout(3000);
		conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
		conn.ignoreContentType(true);
		int num = 0;
		Document doc1 = null;
		while (num < 15) {
			try {
				doc1 = conn.get();
				break;
			} catch (Exception e) {
				num++;
				if (num == 15) {
					try {
						fx.write(url + "error" + "\r");
						fx.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
				try {
					Thread.sleep(200);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		fx = null;
		return doc1;
	}
}
