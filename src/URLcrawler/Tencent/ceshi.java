package URLcrawler.Tencent;

import hbase.HBaseCRUD;

import java.io.FileWriter;
import java.io.IOException;
import org.jsoup.Jsoup;
import org.jsoup.helper.HttpConnection;
import org.jsoup.nodes.Document;


public class ceshi {
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static FileWriter fw;

	public static Document getdoc(String url) {
		FileWriter fx = null;
		try {
			fx = new FileWriter("src/URLcrawler/Tencent/tencentLog.txt", true);
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		HttpConnection conn = (HttpConnection) Jsoup.connect(url);
		conn.timeout(3000);
		conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
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
						fx.write(url + "\r");
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
