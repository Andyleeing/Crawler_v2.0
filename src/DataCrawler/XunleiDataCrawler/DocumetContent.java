/*
 * 
 *  @ 作者：韩嘉星
 *  
 *  @介绍：获取源文件，js文件
 *  
 *  @创建时间：2014.7.28
 *  
 *  @修改记录：
 *        时间：2014.8.1
 *        修改人：韩嘉星
 *        内容：改正异常即捕获又抛出的做法
 *        
 *        时间：2014.8.7
 *        修改人：韩嘉星
 *        内容：修改超时时间，由100秒改为6秒
 *        
 * */
package DataCrawler.XunleiDataCrawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.jsoup.Jsoup;
import org.jsoup.helper.HttpConnection;
import org.jsoup.nodes.Document;


public class DocumetContent {
	public static int a = 0;
	private int count = 0;
	private int countJS = 0;

	public Document getDocument(String strUrl) {
		while (count < 5) {
			try {
				HttpConnection conn = (HttpConnection) Jsoup.connect(strUrl);
				conn.timeout(60000);
				conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
				try {
					if (conn != null) {
						Document content = conn.get();
						return content;
					}
				} catch (IOException e) {
					count++;
				}

			} catch (Exception e) {
				count++;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					count++;
					e1.printStackTrace();
				}
			}

		}
		a++;
//		System.out.println("no content " + strUrl);
//		System.out.println(a);
		return null;

	}

	public String getJsContent(String strUrl) {
		while (countJS < 5) {
			URL url = null;
			try {
				url = new URL(strUrl);
			} catch (MalformedURLException e) {
				countJS++;
				e.printStackTrace();
			}
			HttpURLConnection uc;
			try {
				uc = (HttpURLConnection) url.openConnection();
				uc.setDoInput(true);// 设置是否要从 URL 连接读取数据,默认为true
				uc.setRequestProperty("contentType", "GBK");
				uc.setConnectTimeout(60000);
				uc.setReadTimeout(60000);
				uc.connect();
				if (uc != null) {
					InputStream iputstream = uc.getInputStream();
					BufferedReader in = null;
					try {
						in = new BufferedReader(new InputStreamReader(
								iputstream, "GBK"));
					} catch (UnsupportedEncodingException e) {
						countJS++;
						e.printStackTrace();
					}
					StringBuffer buffer = new StringBuffer();
					String line = "";
					while ((line = in.readLine()) != null) {
						buffer.append(line);
					}
					String str = buffer.toString();
					iputstream.close();
					in.close();
					uc = null;
					buffer = null;
					return str;
				}
			} catch (IOException e) {
				countJS++;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					countJS++;
				}
			}
		}
		a++;
		System.out.println(a);
//		System.out.println("getJS no content   " + strUrl);
		return null;

	}

}
