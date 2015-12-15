package DataCrawler.SohuDataCrawler;

import hbase.HBaseCRUD;

import java.net.URI;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import jxHan.Crawler.Util.Log.ExceptionHandler;

import org.apache.commons.math.MathConfigurationException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Utils.JDBCConnection;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
public class UtilsSohu {
	private static UtilsSohu youkuFun = null;
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static String tablename = "sohuurl";
	public  JHtmlUpdateCheck JU = new JHtmlUpdateCheck();
	public static UtilsSohu getYoukuFun() {
		if (youkuFun == null)
			youkuFun = new UtilsSohu();
		return youkuFun;
	}
 
	
	public  double paidSumplay(String infokey){
		String url="http://tv.sohu.com/"+infokey;
		String source=JU.visitURL(url);
		Pattern p=Pattern.compile("(总播放)(.*?)(</span>)");
		Matcher matcher=p.matcher(source);
		if(matcher.find()){
			String temp=matcher.group().replaceAll("\\s", "");
			temp=temp.replaceAll("\\s", "");
			temp=temp.replaceAll(",", "");
			String result=null;
			double res=0;
			Pattern p2=Pattern.compile("((\\d|\\.){1,})([万|亿]?)");
			Matcher matcher2=p2.matcher(temp);
			if(matcher2.find()){
				result=matcher2.group(1);
				try{
					res=Double.parseDouble(result);
				}catch(NumberFormatException e){
					return -1;
				}
				if(matcher2.group(3).matches("万")){
					return res*10000;
				}else if(matcher2.group(2).matches("亿")){
					return res*100000000;
				}else return res;
				
			}
		}
		return -1;
	}
	public static ArrayList<String> readURLs(String filePath) {
		ArrayList<String> urls = new ArrayList<String>();
		try {
			BufferedReader bw = new BufferedReader(new FileReader(filePath));
			String url = null;
			while ((url = bw.readLine()) != null) {
				if (url != null && !url.equals("")){
//					if(url.indexOf("/item/")>=0)
					urls.add(url);
				}
					
			}
			bw.close();
		} catch (FileNotFoundException e1) {
			System.out.println(filePath);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println(filePath);
			e1.printStackTrace();
		}
		return urls;
	}

	public static String dealSrc(String source) {
		if (source == null || source.equals(""))
			return null;
		source = source.replaceAll("\\s*", "");
		source = source.replace("_", "");
		while (source.indexOf(" ") > 0) {
			System.out.println("still exists");
			source = source.replaceAll("\\s*", "");
		}
		source = source.toLowerCase();
		return source;
	}

	public  String[] getVPid(String url, String category,JDBCConnection jdbc) {
		if (url == null || url.equals(""))
			return null;
		if (category == null || category.equals(""))
			return null;
		String source = JU.visitURL(url);
		if (source == null || source.equals("")) {
			jdbc.log("郑玲", url.substring(url.indexOf("http://tv.sohu.com/")+19), 1, "sh", url, "URL访问失败", 1);
			return null;
		}
		String temp = source;
		temp = UtilsSohu.dealSrc(source);
		String[] id = new String[3];
		int index = 0;
		index = temp.indexOf("vid=\"");
		if (index < 0)
			return null;
		temp = temp.substring(index + 5);
		index = temp.indexOf("\"");
		if (index < 0)
			return null;
		id[0] = temp.substring(0, index);
		try {
			Integer.parseInt(id[0]);
		} catch (Exception e) {
			return null;
		}
		// playlistid
		index = temp.indexOf("playlistid=\"");
		if (index < 0)
			return null;
		temp = temp.substring(index + 12);
		index = temp.indexOf("\"");
		if (index < 0)
			return null;
		id[1] = temp.substring(0, index);
		try {
			Integer.parseInt(id[1]);
		} catch (Exception e) {
			return null;
		}
		id[2] = source;
		if (id == null || id.length < 3)
			return null;
		return id;
	}

	public static String[] movieInfoCount(String source) {
		// String url="http://tv.sohu.com/item/MTEwNjkwMQ==.html";
		// String source=JHtmlUpdateCheck.visitURL(url);
		String playcount, yesterdayCount;
		int index = 0;
		index = source.indexOf("总播放");
		if (index < 0)
			return null;
		String temp;
		temp = source.substring(index + 4);
		index = temp.indexOf("</span>");
		if (index < 0)
			return null;
		playcount = temp.substring(0, index);
		playcount = dealWithCount(playcount);
		index = temp.indexOf("昨日新增播放");
		if (index < 0)
			return null;
		temp = temp.substring(index + 7);
		index = temp.indexOf("</span>");
		if (index < 0)
			return null;
		yesterdayCount = temp.substring(0, index);
		yesterdayCount = dealWithCount(yesterdayCount);
		String[] items = { playcount, yesterdayCount };
		if (items.length < 2 || items == null)
			return null;
		return items;
	}

	public static String dealWithCount(String count) {
		// 1.去除 逗号 2.将万和亿加0
		count = count.replaceAll(",", "");
		count = count.replaceAll("万", "0000");
		count = count.replaceAll("亿", "00000000");
		return count;
	}

	public static String getRowkey(String url) {
		int index = 0;
		index = url.indexOf("com/");
		if (index < 0)
			return null;
		String rowkey = null;
		rowkey = url.substring(index + 4);
		return rowkey;
	}


	public static void empty(String filepath) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));
			writer.write("");
			writer.flush();
			writer.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	
}
