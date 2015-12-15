/*
 * 
 * @ 作者：韩嘉星
 *  
 *  @介绍：获取源文件，js文件
 *  
 *  @创建时间：2014.7.28
 *  
 *  @修改记录：
 *        时间：2014.8.10
 *        修改人：韩嘉星
 *        内容：将超时时间由50秒修改为6秒
 *       
 *        
 * */
package URLcrawler.Xunlei;

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

import Utils.JDBCConnection;

/*
 * 获取源文件
 * 
 */
public class DocumetContent {
	public JDBCConnection jdbconn;
	private int count = 0;

	public DocumetContent(JDBCConnection jdbconn) {
		this.jdbconn = jdbconn;
	}

	public Document getDocument(String strUrl) {
		while (count < 5) {
			try {
				HttpConnection conn = (HttpConnection) Jsoup.connect(strUrl);
				conn.timeout(6000);
				conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
				try {
					Document content = conn.get();
					return content;
				} catch (IOException e) {
					count++;
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				count++;
			}
		}
		jdbconn.log("韩嘉星", "", 1, "xl", strUrl, "url目录页连接错误", 3);
		System.out.println("no content" + strUrl);
		return null;
	}

}
