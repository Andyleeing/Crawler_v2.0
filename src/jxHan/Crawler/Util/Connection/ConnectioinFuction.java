package jxHan.Crawler.Util.Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import jxHan.Crawler.Util.Log.ExceptionHandler;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
public class ConnectioinFuction {
	
	public static String readURL(String url) {
		
		HttpResponse response = null;
		StringBuffer pageBuffer = new StringBuffer();
		BufferedReader reader = null;
		try {
			HttpGet get = initHttpGet(url);
			response = ConnectionPool.getDefaultHttpClient().execute(get);
			reader = new BufferedReader(new InputStreamReader(
					response.getEntity().getContent()));
			String line;
			if(response.getStatusLine().getStatusCode() != 200) {
				return response.getStatusLine().getStatusCode() + "";
			}
			while ((line = reader.readLine()) != null) {
				pageBuffer.append(line);
				if(url.indexOf("v.youku.com/v_show/") >= 0 && pageBuffer.indexOf("<span id=\"subtitle\">") >= 0)
					break;
			}
			
		}catch (Exception e) {
			ExceptionHandler.log(url, e);
		}finally {
			try {
				if(reader != null)
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				ExceptionHandler.log(url, e);
			}
		}
		return pageBuffer.toString();
	}
		public static HttpGet initHttpGet(String url) {
			HttpGet get = new HttpGet(url);
			get.addHeader("Accept", "text/html");
			get.addHeader("Accept-Charset", "utf-8");
			// get.addHeader("Accept-Encoding", "gzip");
			get.addHeader("Accept-Language", "en-US,en");
			get.addHeader(
					"User-Agent",
					"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.160 Safari/537.22");
			return get;
		}
}
