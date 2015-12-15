package URLcrawler.Tencent;

import java.io.IOException;
import java.util.ArrayList;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import URLcrawler.Tencent.ceshi;

public class URLMakercartoon {

	public static ArrayList<String> connect(ArrayList<String> baseUrl)
			throws IOException, InterruptedException {

		String url = "http://v.qq.com/cartlist/0/3_-1_-1_-1_-1_0_0_0_20.html";
		String aurl = "http://v.qq.com/cartlist/";
		String burl = "/3_-1_-1_-1_-1_0_";
		String curl = "_0_20.html";

		
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
		// i涓烘�椤垫暟 j涓轰釜浣嶆暟

		for (int i = 0; i < totalPage; i++) {
			int j = i % 10;
			tempUrl = aurl + j + burl + i + curl;
			baseUrl.add(tempUrl);
		}
		return baseUrl;

	}
}
