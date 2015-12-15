package DataCrawler;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.helper.HttpConnection;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class test {

	public static void main(String args[]) {
	
	HttpConnection conn = (HttpConnection) Jsoup.connect("http://www.daaaaa.com");
	conn.timeout(10000);
	conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
	
	try {
		Document doc;
		doc = conn.get();
	
	Elements allLinkfirst = null;
	allLinkfirst = doc.getElementsByAttributeValue("class",
			"site-piclist_info_title").select("a[href]");// 第一页每个电影的url.
	 } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}// doc指动漫的原网页，此时是第一页strurl。
	
}
}
