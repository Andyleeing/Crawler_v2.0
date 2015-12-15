package URLcrawler.Sohu;

import hbase.HBaseCRUD;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.dom4j.Element;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import jxHan.Crawler.Util.FileHandler;
import jxHan.Crawler.Util.Log.ExceptionHandler;
import jxHan.Crawler.Util.XML.XMLfunction;
import jxHan.Crawler.WebSite.Base.GlobalData;

public class CrawlerThread implements Runnable {
	public long time;
	public static String date;
	public static int dyCount = 0;
	public static HBaseCRUD hbase = new HBaseCRUD();
	public static HashSet<String> urlList = new HashSet<String>();
	public static HashSet<String> tempList = new HashSet<String>();
	public static ArrayList<CrawlerThread> pool;
	public static  FileWriter crawlurl=null;
	public int i = 200;
	public int flag = 1;
	int step;
	public static int countVip=0;
	public static int dianyingCount=0;
	public static ArrayList<String>arrList=new ArrayList<String>();

	public CrawlerThread(long time, String date, ArrayList<CrawlerThread> pool) {
		this.time = time;
		this.date = date;
		this.pool = pool;
	}
	
	public CrawlerThread(){
		super();
	}
	
	public void run() {
		while (true) {
			String item = null;
			String tempUrl = "";
			String category = "";
			String playUrl = "";
			synchronized (arrList) {
				if (arrList != null && arrList.size() > 0) {
					item = arrList.get(0);
					arrList.remove(0);
				} else if  (arrList == null || arrList.size() == 0) {
					synchronized(pool) {
						pool.remove(this);
					}
					System.out.println("remove one thread ");
					break;
				}
			}
			if (item != null && !item.equals("")) {
				String[]splits=item.split("@");
				tempUrl = splits[0];
				category = splits[1];
				if (category.indexOf("dongman")>=0) {//默认info
					getDmPlayUrl(category, tempUrl);
				} else if( category.indexOf("yingshi")>=0){
					getYsPlayUrl(category,tempUrl);
				}
				else if (category.indexOf("dianying")>=0) {//默认 info
					getDyplayUrl(category,tempUrl);
				}else if(category.indexOf("zongyi")>=0){
					getZyPlayUrl(category,tempUrl);
				}
			}
		}

	}

	public String strReverse(String str) {
		StringBuffer sb = new StringBuffer(str);
		sb.reverse();
		return sb.toString();
	}

	public static void getDyplayUrl(String category, String infoUrl) {
		if (infoUrl == null || infoUrl.equals(""))
			return;
		String source = null;
		source = Utils.visitUrl(infoUrl);
		if (source == null || source.equals(""))
			return;
		source = Utils.visitUrl(infoUrl);
		String info[]=findUrl(source);
		if(info==null)return;
		String yugaoUrl=info[0];
		String zhengpianUrl=info[1];
		String vip=info[2];
		synchronized (CrawlerThread.tempList) {
			if (CrawlerThread.tempList.add(zhengpianUrl)||CrawlerThread.tempList.add(yugaoUrl)) {
				String infoKey = infoUrl.substring(infoUrl.indexOf("com/") + 4);
				if (yugaoUrl != null&&!yugaoUrl.equals("")&&yugaoUrl.indexOf("v.tv.sohu.com")<0) {
					String yugaoKey = yugaoUrl.substring(yugaoUrl
							.indexOf("com/") + 4);
					Utils.putSohuUrl(yugaoKey, yugaoUrl + "@dianyingyugao@play@"
							+ infoKey);
				}
				if (zhengpianUrl != null&&!zhengpianUrl.equals("")) {
					String zhengpianKey = zhengpianUrl.substring(zhengpianUrl
							.indexOf("com/") + 4);
					if(vip!=null&&!vip.equals("")&&vip.equals("vip"))
						
						{zhengpianUrl=zhengpianUrl+"###paid";
					     System.out.print(countVip+++"vip"+new Date().getDay());
						}
					Utils.putSohuUrl(zhengpianKey, zhengpianUrl
							+ "@dianyingzhengpian@play@" + infoKey);
				}
				if(yugaoUrl==null&&zhengpianUrl==null)System.out.println("noplay-->"+infoUrl);

			}
		}
	}

	public static String findYugao(String source){
		if(source==null||source.isEmpty())return null;
		String temp=null;
		int index1=0;
		int index2=0;
		index1=source.indexOf("cfix bot");
		if(index1<0)return null;
		index2=source.indexOf("播放预告片");
		if(index2<0)return null;
		temp=source.substring(index1,index2);
		index1=temp.indexOf("href");
		index2=temp.indexOf("html");
		if(index1<0||index2<0)return null;
		temp=temp.substring(index1+6,index2+4);
//		System.out.println(temp);
		return temp;
		}
	public static String findZhengpianOnly(String source){
		if(source==null||source.isEmpty())return null;
		Pattern p=Pattern.compile("cfix bot.*播放正片", Pattern.CASE_INSENSITIVE);
		Matcher m=p.matcher(source);
		if(m.find())
		{   
			String temp=m.group();
			Pattern p2=Pattern.compile("http://.*html", Pattern.CASE_INSENSITIVE);
			Matcher m2=p2.matcher(temp);
			if(!m2.find()) return null;
			Pattern p3=Pattern.compile("播放正片.*免费看");
			Matcher m3=p3.matcher(source);
			if(m3.find())
				return m2.group()+"###paid";
			else return m2.group();
		}
		
		return null;
		}
	
	public static String[] findUrl(String source){
		String[] info={"","",""};
//		System.out.print(info.length);
		if(source==null||source.isEmpty())return null;
		Pattern p=Pattern.compile("cfix bot.*btn-playPre");
		Pattern p2=Pattern.compile("cfix bot.*btn-playFea");
		Pattern p3=Pattern.compile("btn-playPre.*btn-playFea");
		Pattern p5=Pattern.compile("cfix bot.*work_info_vip");
		Pattern p4=Pattern.compile("http://.*html");
		Matcher m=p.matcher(source);
		Matcher m2=null;
		if(m.find()){	//yugao
			m2=p4.matcher(m.group());
			if(m2.find()){
			System.out.println("yugao--"+m2.group());
			info[0]=m2.group();
			}
			m=p3.matcher(source);
			if(m.find())
			{
				m2=p4.matcher(m.group());
				if(m2.find()){
				System.out.println("zhengpian--"+m2.group());
				info[1]=m2.group();
				}
			}
			
		}else{
		m=p2.matcher(source);
		if(m.find()){
			m2=p4.matcher(m.group());
			if(m2.find()){
			System.out.println("zhengpian--"+m2.group());
			info[1]=m2.group();
			}
		}
		}
		m=p5.matcher(source);
		if(m.find()){
			System.out.println("vip");
			info[2]="vip";
		}
		return info;
	}
	public static String findZhengpian(String source){
		if(source==null||source.isEmpty())return null;
		Pattern p=Pattern.compile("播放预告片.*播放正片", Pattern.CASE_INSENSITIVE);
		Matcher m=p.matcher(source);
		if(m.find())
		{   
			String temp=m.group();
			Pattern p2=Pattern.compile("http://.*html", Pattern.CASE_INSENSITIVE);
			Matcher m2=p2.matcher(temp);
			if(!m2.find()) return null;
			Pattern p3=Pattern.compile("播放正片.*免费看");
			Matcher m3=p3.matcher(source);
			if(m3.find())
				return m2.group()+"###paid";
			else return m2.group();
		}
		return null;
	}
	public static  void getZyPlayUrl(String category,String infoUrl){
		String playUrl = null;
		String content = null;
		content = Utils.visitUrl(infoUrl);
		String temp = content;
		if (infoUrl == null || infoUrl.equals(""))return;
		int index1 = 0;
		int index2=0;
		index1 = temp.indexOf("plot-rtit");
		while (index1 > -1) {
			temp=temp.substring(index1+10);
			index2=temp.indexOf("href=\"http");
			if(index2<0)break;
			index1=index2+6;
			index2=temp.indexOf("html")+4;
			if (index1<0||index2<0)break;
			playUrl = temp.substring(index1,index2);
			if (playUrl.length() >= 100)return;
			synchronized (CrawlerThread.tempList) {
				if (CrawlerThread.tempList.add(playUrl)) {
					String key = playUrl.substring(playUrl.indexOf("com/") + 4);
					String infoKey = infoUrl.substring(infoUrl.indexOf("com/") + 4);
//					Utils.printData(playUrl + "@" + category + "@play@" + infoKey );
					Utils.putSohuUrl(key, playUrl + "@" + category + "play"
							+ "@play@" + infoKey);
				}
			}
			index1 = temp.indexOf("plot-rtit");
		}
	}
	public static  void getYsPlayUrl(String category, String infoUrl) {
//		int indexid = 0;
		String content = null;
		content = Utils.visitUrl(infoUrl);
		String temp = content;
		if (infoUrl == null || infoUrl.equals(""))return;
		int index1 = 0;
		int index2=0;
		index1=temp.indexOf("up_g_page_container");
		index2=temp.indexOf("bottom_p_page_container");
		if (index1<0||index2<0)return;
		temp=temp.substring(index1, index2);
		String playUrl = null;
		index1 = temp.indexOf("<div class=\"pic\">");
		while (index1 > -1) {
			temp=temp.substring(index1+10);
			index2=temp.indexOf("href=\"http");
			if(index2<0)break;
			index1=index2+6;
			index2=temp.indexOf("html")+4;
			if (index1<0||index2<0)break;
			playUrl = temp.substring(index1,index2);
			if (playUrl.length() >= 100)return;
			if(playUrl.indexOf("store.tv.sohu.com")>=0)return;
			synchronized (CrawlerThread.tempList) {
				if (CrawlerThread.tempList.add(playUrl)) {
					String key = playUrl.substring(playUrl.indexOf("com/") + 4);
					String infoKey = infoUrl.substring(infoUrl.indexOf("com/") + 4);
					Utils.putSohuUrl(key, playUrl + "@" + category + "play"
							+ "@play@" + infoKey);
				}
			}
			index1 = temp.indexOf("<div class=\"pic\">");
		}
	}
	public static  void getDmPlayUrl(String category, String infoUrl) {
		String content = null;
		content = Utils.visitUrl(infoUrl);
		String temp = content;
		if (infoUrl == null || infoUrl.equals(""))return;
		int index1 = 0;
		int index2 = 0;
		index1=temp.indexOf("similarLists");
		if(index1<0)return;
		temp=temp.substring(index1+12);
		index2=temp.indexOf("similarLists");
		if (index2<0)return;
		temp=temp.substring(0, index2);
		String playUrl = null;
		index1=temp.indexOf("<li clear='21'>");
		while (index1 > -1) {
			temp=temp.substring(index1+7);
			index2=temp.indexOf("html")+4;
			index1=temp.indexOf("href= '")+7;
			playUrl = temp.substring(index1,index2);
			if (playUrl.length() >= 100)return;
			synchronized (CrawlerThread.tempList) {
				if (CrawlerThread.tempList.add(playUrl)) {
					String key = playUrl.substring(playUrl.indexOf("com/") + 4);
					String infoKey = infoUrl.substring(infoUrl.indexOf("com/") + 4);
					Utils.putSohuUrl(key, playUrl + "@" + category + "play"
							+ "@play@" + infoKey);
				}
			}
			index1=temp.indexOf("<li clear='21'>");
		}
	}
	
}
