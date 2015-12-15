package URLcrawler.URLcrawler_56;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;

public class Share
{
	public static int Count=0;
	public static String visitURL(String infoId,String href,String category,String showtype)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		while (true) 
		{
			content = ConnectioinFuction.readURL(href);//获取url的内容content;用httpClient或Jsoup实现；
			if (content != null && !content.equals(""))//content对象非空，且里面的内容非空，即有内容，则退出循环；
				break;
			try {
				Thread.sleep(100);//线程休眠，减少403错误
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
			if (count == 3) //总是出现403错误,多次连接，或增加超时连接时间,若还是无内容，这进行异常处理。
			{
				CrawlerThread.urlListInfo.add(infoId+"@"+href+"@"+category+"@Info"+"@"+showtype);	
				//ExceptionHandler.log(href + " noContent", null);
				break;
			}
		}
		return content;
	}
	
	public static String visitUrl2(String href)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		while (true) 
		{
			content = ConnectioinFuction.readURL(href);//获取url的内容content;用httpClient或Jsoup实现；
			if (content != null && !content.equals(""))//content对象非空，且里面的内容非空，即有内容，则退出循环；
				break;
			try {
				Thread.sleep(100);//线程休眠，减少403错误
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
			if (count == 100) //总是出现403错误,多次连接，或增加超时连接时间,若还是无内容，这进行异常处理。
			{
				//ExceptionHandler.log(href + " noContent", null);
				break;
			}
		}
		return content;
	}
	
	public static String visitUrl3(String url)
	{
		String content=null;
		int count=0;
		while(true)
		{
			try
			{
				Document doc=Jsoup.connect(url).timeout(1000).userAgent("Mozilla/5.0 (Windows NT 6.1;WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
				content=doc.toString();
				content=content.replaceAll("\n","");
				break;
			}
			catch(IOException e)
			{
				count++;
				if(count==100)
				{
					break;
				}
			}
		}
		return content;
	}
	
	
	public static String strFind(String str,String tagStart,String tagEnd)//串查找：查找两个字符串中间的子串
	{
		int indexStart=0;
		int indexEnd=0;
		String subString = null;
		indexStart=str.indexOf(tagStart)+tagStart.length();
		indexEnd=str.indexOf(tagEnd,indexStart);
		if(indexStart>=0&&indexEnd>=0)
		subString=str.substring(indexStart, indexEnd);
		return subString;
	}
	
	public static ArrayList<String> getPageUrl(String url0,String url1,ArrayList<String> url,String showtype) throws IOException //拼凑每个网页的url
	{
		String contentFirstPage=visitUrl3(url0); 
		
		if(contentFirstPage==null) 
		{
			JDBCConnection logc = new JDBCConnection();
			logc.log("师玉龙", url0+"56", 1, "56", url0, "get page url failed", 3);
			logc.closeConn();	
			return url;  //modify at 9.8
		}
		
		Document docPageNum=Jsoup.parse(contentFirstPage);
		//System.out.println("docPageNum="+docPageNum);
		Element elePageNum=docPageNum.select("[class=mod56_page_total_page]").first();//选取Elements中的第一个Element，其实就一个，多个用for遍历。
		//System.out.println("elePageNum="+elePageNum);
		if(elePageNum!=null) { //modify at 9.7 
		String textPageNum=elePageNum.text();
		String strPageNum=textPageNum.substring(textPageNum.indexOf("共")+1,textPageNum.indexOf("页"));
		int pageNum=Integer.valueOf(strPageNum);
		int url2=0;
		String url3=".html";
		for(int i=1;i<=pageNum;i++)
		{
			url2=i;
			url.add(url1+url2+url3+"@"+showtype);//保存url	
			
			//System.out.println(url1+url2+url3+"@"+showtype);
		}		
		}
		return url;
		
	}
	
	public static ArrayList<String> getInfoUrl(ArrayList<String> pageUrl,ArrayList<String> urlListInfo,String category,Date date) throws IOException //提取信息页的url
	{
		String content=null;
		Document doc=null;
		Elements eles=null;
		String url=null;
		String id=null;
		String store=null;
		int num;
		int line;
		int column;
		String name="";
		int len=pageUrl.size();
		String[] str=null;
		for(int i=0;i<len;i++)
		{
			str=pageUrl.get(i).split("@");
			
			//System.out.println(pageUrl.get(i));
			content=visitUrl3(str[0]);
			
			if(content==null) 
			{
				JDBCConnection logc = new JDBCConnection();
				logc.log("师玉龙", str[0]+"56", 1, "56", str[0], "get info url failed", 3);
				logc.closeConn();				
			}
			
			doc=Jsoup.parse(content);
			eles=doc.select("[class=vtitle]");
			//num=1;
			for(Element ele:eles)
			{
				url=ele.attr("href");
//				name=ele.attr("title");
//				if(name.charAt(name.length()-1)==' ') name=name.substring(0, name.length()-1);
				
				if(url.contains("v_")) 
				{
					id=url.substring(url.lastIndexOf("_")+1,url.lastIndexOf("."));
					store=id+"@"+url+"@"+category+"@Play"+"@-1"+"@"+str[1];//没有info页的infoId为-1
					CrawlerThread.urlListPlay.add(store);
				}
				else 
				{
					id=url.substring(url.lastIndexOf("/")+1,url.lastIndexOf("."));
					store=id+"@"+url+"@"+category+"@Info"+"@"+str[1];//url为info页的id全为数字
					urlListInfo.add(store);
//					System.out.println(urlListInfo.size());
//					System.out.println(store);
					
				}
				//num++;
			}
		}
		return urlListInfo;
	}
	
	public static void getPlayUrl(String infoId,String infoUrl,String category,String showtype,String key) //提取播放页的url
	{
		String playUrl=null;
		String content=null;
		Document doc=null;
		Elements elesZhengPian=null;
		Elements elesYuGaoPian=null;
		String urlZhengPian=null;
		String urlYuGaoPian=null;
		String store=null;
		String playId=null;
		content=visitURL(infoId,infoUrl,category,showtype);
		
		if(content==null) 
		{
			JDBCConnection logc = new JDBCConnection();
			logc.log("师玉龙", infoUrl+"56", 1, "56", infoUrl, "get play url failed", 3);
			logc.closeConn();				
		}
	
		doc=Jsoup.parse(content);
		elesZhengPian=doc.select(key);
		if(elesZhengPian.size()==0) elesZhengPian=doc.select("[class=play_btn]");//是动漫时只有1集的处理
		if(key.equals("[class=episode]")) elesZhengPian=elesZhengPian.select("a");//是电视时的2层选择（子元素），动漫多集。
		for(Element ele:elesZhengPian)
		{
			urlZhengPian=ele.attr("href");
			if(urlZhengPian.equals("javascript:;")) continue;//排除掉非法的href，不能用==比较两个字符串是否相等；
			playId=urlZhengPian.substring(urlZhengPian.lastIndexOf("_")+1,urlZhengPian.lastIndexOf("."));
			store=playId+"@"+urlZhengPian+"@"+category+"@Play"+"@"+infoId+"@"+showtype;	
			CrawlerThread.urlListPlay.add(store);	
			//System.out.println(store);
			Count++;
			System.out.println("Count="+Count);
		}
		elesYuGaoPian=doc.select("[class=so_list so_list_s3 clearfix]").select("[class=so_pic]");//2次选择；
		for(Element ele:elesYuGaoPian)
		{
			urlYuGaoPian=ele.attr("href");
			if(urlYuGaoPian.equals("javascript:;")) continue;
			playId=urlYuGaoPian.substring(urlYuGaoPian.lastIndexOf("_")+1,urlYuGaoPian.lastIndexOf("."));
			store=playId+"@"+urlYuGaoPian+"@"+category+"@Play"+"@"+infoId+"@"+showtype;
			CrawlerThread.urlListPlay.add(store);
			//System.out.println(store);
		}	
	}
	
}	

	
