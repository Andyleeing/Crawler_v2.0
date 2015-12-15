package URLcrawler.Tencent;

import java.util.ArrayList;

import hbase.HBaseCRUD;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import Utils.TextValue;

public class OutPutFilecarton {

	HBaseCRUD hbase;

	public OutPutFilecarton(HBaseCRUD hbase) {
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
				String fileName = temp.substring(idIndex + 8, idIndex + 23); // 每个原网页保存的文件名
				String save = temp + " + " + score + " * type:cartoon"; // save是加上换行符的url
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
				} catch (Exception e1) {
					e1.printStackTrace();
				}

				Document doc1 = ceshi.getdoc(temp);
				Elements allLink2 = doc1.getElementsByAttributeValue("_hot",
						"coverv2.vlisttab.title");
				for (Element link2 : allLink2) {
					String endUrl = link2.attr("abs:href");
					String title = link2.attr("title");
					save = endUrl + "  " + title + " * type:cartoon";

					idIndex = endUrl.indexOf("cover");
					key = endUrl.substring(idIndex + 8,
							endUrl.indexOf(".html", idIndex + 8));
					rows[0] = key;
					values[0] = "tencent " + save;
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
					// file.write(save.getBytes()); // 得到每个视频里面每一集的url
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
