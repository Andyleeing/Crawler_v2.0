package DataCrawler.SohuDataCrawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HeaderElement;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.cookie.MalformedCookieException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.util.TimeoutController.TimeoutException;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;


public class JHtmlUpdateCheck {

	public  String getHtmlContent(URL url, String encode) {
		StringBuffer contentBuffer = new StringBuffer();
		int responseCode = -1;
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) url.openConnection();
			con.setConnectTimeout(50000);
			con.setRequestProperty("User-Agent",
					"Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");
			responseCode = con.getResponseCode();
			if (responseCode == -1) {
				con.disconnect();
				return null;
			}
			if (responseCode >= 400) // ����ʧ��
			{
				con.disconnect();
				return null;
			}
			InputStream inStr = con.getInputStream();
			InputStreamReader istreamReader = new InputStreamReader(inStr,
					encode);
			BufferedReader buffStr = new BufferedReader(istreamReader);
			String str = null;
			while ((str = buffStr.readLine()) != null)
				contentBuffer.append(str);
			inStr.close();
		} catch (SocketTimeoutException e) {
			e.printStackTrace();
			// System.out.println(e.getMessage());
			contentBuffer = null;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			contentBuffer = null;
			// System.out.println(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			e.printStackTrace();
			contentBuffer = null;
			// System.out.println(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			// System.out.println(e.getMessage());
			contentBuffer = null;
			e.printStackTrace();
		} finally {

			con.disconnect();
		}
		if (contentBuffer == null)
			return null;
		return contentBuffer.toString();
	}

//	public static void main(String[] args) {
//		System.out.println(visitURL("http://tv.sohu.com/20130829/n385338511.shtml"));
//	}

	public  String getSource(URL url, String encode) {
		if (url == null || url.equals(""))
			return null;
		String source = getHtmlContent(url, encode);
		int count = 0;
		while (source == null) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			count++;
			if (count >= 5) {
				break;
			}
			source = getHtmlContent(url, encode);

		}
		return source;
	}

	public  String visitURL(String url) {
		if (url == null || url.equals(""))
			return null;
		String encode = null;
		encode = getEncoding(url);
		int countEncode = 0;
		while (encode == null) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			countEncode++;
			if (countEncode > 5) {
//				synchronized (CrawlSohu.fwURL) {
//					try {
//						CrawlSohu.fwURL.write(CrawlSohu.encodeNULL++
//								+ "encode null-->" + url.toString() + "\n");
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
				return null;
			} else
				encode = getEncoding(url);
		}
		if (!url.toLowerCase().startsWith("http://")) {
			url = "http://" + url;
		}
		URL rUrl = null;
		try {
			rUrl = new URL(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return getSource(rUrl, encode);
	}

	public String getEncoding(String strUrl) {
		String strEncoding = null;
		HttpClient client = new HttpClient();
		int timeout = 50000;
		client.getHttpConnectionManager().getParams()
				.setConnectionTimeout(timeout);
		client.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
		GetMethod method = new GetMethod(strUrl);
		method.setFollowRedirects(true);
		int statusCode = 0;
		try {
			statusCode = client.executeMethod(method);
		} catch (HttpException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		if (statusCode != -1) {
			strEncoding = getContentCharSet(method
					.getResponseHeader("Content-Type"));
			if (strEncoding == null || strEncoding.equals("")) {
				strEncoding = "GBK";
			}
			method.releaseConnection();
			return strEncoding;

		}
		method.releaseConnection();
		return null;
	}

	// public static String getEncoding(String strURL) {
	// URL url;
	// Map<String, List<String>> map = null;
	// try {
	// url = new URL(strURL);
	// HttpURLConnection urlConnection = (HttpURLConnection)
	// url.openConnection();
	// map = urlConnection.getHeaderFields();
	// } catch (MalformedURLException e) {
	// e.printStackTrace();
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// String strencoding = null;
	// Set<String> keys = map.keySet();
	// Iterator<String> iterator = keys.iterator();
	// String key = null;
	// String tmp = null;
	// while (iterator.hasNext()) {
	// key = iterator.next();
	// tmp = map.get(key).toString().toLowerCase();
	// if (key != null && key.equals("Content-Type")) {
	// strencoding = regCharset(tmp, strURL);
	// }
	// }
	// return strencoding;
	//
	// }

	public String regCharset(String charsetStr, String strURL) {
		String code = null;
		Pattern p = Pattern.compile("(charset=)(.*)]");
		Matcher m = p.matcher(charsetStr);
		if (m.find()) {
			code = m.group(2);

		} else {
			code = "gbk";
		}
		return code;
	}

	protected  String getContentCharSet(Header contentheader) {

		String charset = null;

		if (contentheader != null) {

			HeaderElement values[] = contentheader.getElements();

			if (values.length == 1) {

				NameValuePair param = values[0].getParameterByName("charset");

				if (param != null) {

					charset = param.getValue();

				}

			}

		}

		return charset;

	}

}
