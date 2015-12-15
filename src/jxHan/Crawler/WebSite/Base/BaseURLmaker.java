package jxHan.Crawler.WebSite.Base;
public class BaseURLmaker {
	
	public static String generateURL(String[] args) {
		String URL_final = GlobalData.baseURL;
		for(int i = 0;i < args.length;i++) {
			URL_final += args[i];
		}
		return URL_final;
	}
}
