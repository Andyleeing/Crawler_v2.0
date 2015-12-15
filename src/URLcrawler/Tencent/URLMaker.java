package URLcrawler.Tencent;

import java.io.IOException;
import java.util.ArrayList;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import URLcrawler.Tencent.ceshi;

public class URLMaker {

	public static ArrayList<String> connect(ArrayList<String> baseUrl) throws IOException, InterruptedException{
		
		String url = "http://v.qq.com/movielist/1/0/0/0/0/20/0/0.html";
		String aurl="http://v.qq.com/movielist/1/0/0/0/";
		String burl="/20/0/0.html";
				
		Document doc = ceshi.getdoc(url);
		Element page = null;
		try{
			page = doc.getElementsByClass("c_txt6").last();
		}catch(Exception e){
			return baseUrl;
		}
		String temp = page.attr("title"); 
		int totalPage = Integer.parseInt(temp);
		String tempUrl = null;
		
		for(int i=0; i<totalPage; i++) {
			tempUrl = aurl+i+burl;
			baseUrl.add(tempUrl);
		}
		return baseUrl;

	}
}
