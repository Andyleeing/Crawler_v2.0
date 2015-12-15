package URLcrawler.Leshi;

import hbase.HBaseCRUD;

import java.util.ArrayList;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class LeshiFunction {
	private static LeshiFunction leshiFun = null;
	public static int threadCount = 10;
	public HBaseCRUD hbase = new HBaseCRUD();

	public static LeshiFunction getLeshiFun() {
		if (leshiFun == null)
			leshiFun = new LeshiFunction();
		return leshiFun;
	}

	public void movieurl() {
		ArrayList<String> menulist = new ArrayList<String>();
		for (int type = 1; type < 4; type = type + 2) {
			String url = "http://list.letv.com/listn/c1_t_a_y_s" + type
					+ "_lg_ph_md_o_d_p.html";
			String content = visitURL(url);
			Document doc = Jsoup.parse(content);
			Elements movienum = doc.getElementsByClass("result_num");
			String num = movienum.text().replaceAll("[\u4e00-\u9fa5]+", "");
			int sum=0;
			try{
				sum = Integer.parseInt(num);
			}
			catch(Exception e){
				continue;
			}
			if (sum > 0) {
				sum = sum / 30 + 1;
				for (int page = 1; page <= sum; page++) {
					String pageurl = url.substring(0, url.indexOf(".html"))
							+ page + ".html";
					menulist.add(pageurl);
				}
				LeshiPlayCrawler(menulist, 2);
			}

		}
	}

	public static void LeshiPlayCrawler(ArrayList<String> list, int step) {
		ArrayList<leshiThread> pool = new ArrayList<leshiThread>();
		leshiThread.urlList = list;
		for (int i = 0; i < threadCount; i++) {
			leshiThread leshiThread = new leshiThread(pool, step);
			pool.add(leshiThread);
			new Thread(leshiThread).start();
			leshiThread=null;
		}
		while (true) {
			synchronized (pool) {
				if (pool.size() <= 0) {
					break;
				}
			}
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public ArrayList<String> urlmaker(String url) { // 从目录页获取电视剧动漫综艺Info链接
		ArrayList<String> list = new ArrayList<String>();
		String content = visitURL(url);
		Document doc = Jsoup.parse(content);
		Elements movienum = doc.getElementsByClass("result_num");
		String num = movienum.text().replaceAll("[\u4e00-\u9fa5]+", "");
		
		int sum=0;
		try{
			sum = Integer.parseInt(num);
		}catch(Exception e){
			return urlmaker(url);
		}
		if (sum > 0) {
			sum = sum / 30 + 1;
			for (int page = 1; page <= sum; page++) {
				String pageurl = url.substring(0, url.indexOf(".html")) + page
						+ ".html";
				String pageContent = visitURL(pageurl);
				Document doc1 = Jsoup.parse(pageContent);
				Elements allLink = doc1.getElementsByClass("P_t").select(
						"a[href]");
				for (Element link : allLink) {
					if (link != null) {
						String temp = link.attr("abs:href");
						list.add(temp);
					}
				}
			}
		}
		return list;
	}

	private static String visitURL(String href) {
		String content = null;
		int count = 0;
		while (true) {
			content = ConnectioinFuction.readURL(href);
			if (content != null && !content.equals(""))
				break;
			count++;
			if (count == 5) {
				ExceptionHandler.log(href + " noContent", null);
				break;
			}
			try{
				Thread.sleep(1000);			
			}
			catch(Exception e){		
			}
		}
		return content;
	}
}
