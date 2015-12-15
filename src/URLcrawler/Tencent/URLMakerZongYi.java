package URLcrawler.Tencent;

import java.util.ArrayList;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import URLcrawler.Tencent.ceshi;

public class URLMakerZongYi {
	public static ArrayList<String> connect(ArrayList<String> baseUrl) {
		String url = "http://v.qq.com/variety/type/list_-1_0_0.html";
		String aurl = "http://v.qq.com/variety/type/list_-1_0_";
		String burl = ".html";

		Document doc = ceshi.getdoc(url);
		Element page = null;
		try{
			page = doc.getElementsByClass("c_txt6").last();
		}catch(Exception e){
			return baseUrl ;
		}
		String temp = page.attr("title");
		int totalPage = Integer.parseInt(temp);
		String tempUrl = null;

		for (int i = 0; i < totalPage; i++) {
			tempUrl = aurl + i + burl;
			baseUrl.add(tempUrl);
		}
		return baseUrl;

	}
}
