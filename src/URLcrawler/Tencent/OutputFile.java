package URLcrawler.Tencent;

import hbase.HBaseCRUD;

import java.util.ArrayList;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import Utils.TextValue;

public class OutputFile {
	HBaseCRUD hbase ;

	OutputFile(HBaseCRUD hbase) {
		this.hbase = hbase;
	}

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
			Elements allLink = doc.getElementsByClass("scores");

			for (Element link : allLink) {
				String temp = link.toString();
				int aPos = temp.indexOf("http:");
				int bPos = temp.indexOf("title");
				int ascorePos = temp.indexOf("c_txt3");
				int bscorePos = temp.indexOf("</strong");
				String score = temp.substring(ascorePos + 8, bscorePos);
				String everyUrl = temp.substring(aPos, bPos - 2);
				temp = everyUrl;
				int idIndex = temp.indexOf("cover", 0);
				String fileName = temp.substring(idIndex + 8, idIndex + 23); // cid
				String save = temp + " + " + score + " * type:movie"; // save即是每个视频url
				String key = temp.substring(idIndex + 8,
						temp.indexOf(".html", idIndex + 8));
				String tableName = "tencentnew";
				String[] rows = { key };
				String[] colfams = { "C" };
				String[] quals = { "url" };
				String[] values = { "tencent " + save };
				try {
					hbase.putRows(tableName, rows, colfams, quals, values);

					ArrayList<TextValue> values2 = new ArrayList<TextValue>();
					TextValue rowkeytv = new TextValue();
					rowkeytv.text = "rowkey";
					rowkeytv.value = key;
					values2.add(rowkeytv);

					TextValue urltv = new TextValue();
					urltv.text = "url";
					urltv.value = save;
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

				Document doc1 = ceshi.getdoc(temp);
				if (doc1 != null) {
					String page = doc1.toString();
					if (!page.contains("figure_title figure_title_two_row")
							&& !temp.contains("film")) {
						temp = temp.replace("cover", "prev");
						doc1 = ceshi.getdoc(temp);
						if (doc1 == null) {
							jdbc.log("石嘉帆", "", 1, "tx", strUrl, "url地址抓取异常", 3);
							return;
						}
					}
				} else {
					return;
				}
				if (temp.indexOf("film") < 0) { // 普通电影
					Elements allLink2 = null;
					try {
						allLink2 = doc1.getElementsByAttributeValue("class",
								"figure_title figure_title_two_row");
					} catch (Exception e) {
					}
					for (Element link2 : allLink2) {
						String tempLink = link2.toString();
						// 提取出来url
						int urlBegin = tempLink.indexOf("href");
						int urlEnd = tempLink.indexOf(".html");
						String endUrl = "";
						if (urlBegin < urlEnd) {
							endUrl = tempLink.substring(urlBegin + 6,
									urlEnd + 5);
							if (!endUrl.contains("http")) {
								endUrl = "http://v.qq.com" + endUrl;
							}
						}
						// 提取出来视频名称
						int atitle = tempLink.indexOf("title=\"");
						int btitle = tempLink.indexOf("\"", atitle + 7);
						String title = "";
						if (atitle + 7 < btitle) {
							title = tempLink.substring(atitle + 7, btitle);
						}
						save = endUrl + "  " + title + " * type:movie";
						System.out.println(save);

						idIndex = endUrl.indexOf("cover");
						key = endUrl.substring(idIndex + 8,
								endUrl.indexOf(".html", idIndex + 8));
						rows[0] = key;
						values[0] = "tencent " + save;
						try {
							hbase.putRows(tableName, rows, colfams, quals,
									values);
							ArrayList<TextValue> values2 = new ArrayList<TextValue>();
							TextValue rowkeytv = new TextValue();
							rowkeytv.text = "rowkey";
							rowkeytv.value = key;
							values2.add(rowkeytv);

							TextValue urltv = new TextValue();
							urltv.text = "url";
							urltv.value = save;
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
						// file.write(save.getBytes()); // 得到每个视频里面每一集的url
					}

					allLink2 = doc1.getElementsByAttributeValue("_hot",
							"coverv2.fragstab.title");
					for (Element link2 : allLink2) {
						String endUrl = link2.attr("abs:href");
						String title = link2.attr("title");
						// 防止没有域名的情况
						if (!endUrl.contains("http")) {
							endUrl = "http://v.qq.com" + endUrl;
						}
						save = endUrl + "  " + title + " * type:movie";
						System.out.println(save);
						idIndex = endUrl.indexOf("cover");
						key = endUrl.substring(idIndex + 8,
								endUrl.indexOf(".html", idIndex + 8));
						rows[0] = key;
						values[0] = "tencent " + save;
						try {
							hbase.putRows(tableName, rows, colfams, quals,
									values);
							ArrayList<TextValue> values2 = new ArrayList<TextValue>();
							TextValue rowkeytv = new TextValue();
							rowkeytv.text = "rowkey";
							rowkeytv.value = key;
							values2.add(rowkeytv);

							TextValue urltv = new TextValue();
							urltv.text = "url";
							urltv.value = save;
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
						// file.write(save.getBytes()); // 得到每个视频里面每一集的url
					}
				} else { // 影院
					try {
						Elements allLink2 = doc1.getElementsByAttributeValue(
								"_hot", "cover.relate.title");
						// Elements allLink2 =
						// doc1.getElementsByClass("cover_wrap");
						for (Element link2 : allLink2) {
							String endUrl = link2.attr("abs:href"); // 取出url
							// 取出字符串
							String linkString = link2.toString();
							int atitle = linkString.indexOf("title=\"");
							int btitle = linkString.indexOf("\">", atitle);
							String title = "";
							if (atitle + 7 < btitle) {
								title = linkString
										.substring(atitle + 7, btitle);
							}

							// String title = link2.attr("alt");

							save = endUrl + "  " + title + " * type:movie";
							// System.out.println(save);
							idIndex = endUrl.indexOf("cover");
							key = endUrl.substring(idIndex + 8,
									endUrl.indexOf(".html", idIndex + 8));
							rows[0] = key;
							values[0] = "tencent " + save;
							try {
								hbase.putRows(tableName, rows, colfams, quals,
										values);
								ArrayList<TextValue> values2 = new ArrayList<TextValue>();
								TextValue rowkeytv = new TextValue();
								rowkeytv.text = "rowkey";
								rowkeytv.value = key;
								values2.add(rowkeytv);

								TextValue urltv = new TextValue();
								urltv.text = "url";
								urltv.value = save;
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
							// file.write(save.getBytes()); // 得到每个视频里面每一集的url
						}
					} catch (Exception e) {
						// 没有分集的情况
					}
				}
			}
			try {
				jdbc.closeConn();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (Exception e) {

		}
	}
}
