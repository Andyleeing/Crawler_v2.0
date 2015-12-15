package DataCrawler.DataCrawler_56;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.httpclient.HttpConnection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import DataCrawler.CrawlerThread;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;

public class Share
{
	public static String visitUrl(String id,String url,String category,String kind)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		while (true) 
		{
			content = ConnectioinFuction.readURL(url);//获取url的内容content;用httpClient或Jsoup实现；
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
			    break;
			}
		}
		return content;
	}
	
	public static String visitUrlInfo(String id,String url,String category,String kind,String showtype,int flag,JDBCConnection jdbc)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		
		while(true) 
		{
			try 
			{
				Document doc=Jsoup.connect(url).timeout(1000).userAgent("Mozilla/5.0 (Windows NT 6.1;WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
				content=doc.toString();
				content=content.replaceAll("\n","");// to one line
				break;
			} 
			catch (IOException e1) 
			{
				count++;
				
				//System.out.println("info count="+count);
				
				if (count == 3) 
				{
					if(flag<=10)
					{ 
						flag++;
						
						//System.out.println("info flag="+flag);
						
						synchronized(CrawlerThread.urlList)
						{
							CrawlerThread.urlList.add(id+"@"+url+"@"+category+"@"+kind+"@"+showtype+"@"+flag);
						}		
					}
					else 
					{
						jdbc.log("师玉龙", "56+"+id, 1, "56", url, "no content", 1);
						break;
					}
					
					break;
				}
				try
				{
					Thread.sleep(100);
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		return content;
	}
	
	public static String visitUrlPlay(String id,String url,String category,String kind,String infoId,String showtype,int flag,JDBCConnection jdbc)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		
		while(true) 
		{
			try 
			{
				Document doc=Jsoup.connect(url).timeout(1000).userAgent("Mozilla/5.0 (Windows NT 6.1;WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
				content=doc.toString();
				content=content.replaceAll("\n","");
				
				//System.out.println("content="+content);
				
				doc = null;
				break;
			} 
			catch (IOException e1) 
			{
				//ExceptionHandler.log(url, e1);
				count++;
				
				//System.out.println("play count="+count);
				
				if (count == 3) //总是出现403错误,多次连接，或增加超时连接时间,若还是无内容，这进行异常处理。
				{
					if(flag<=10)
					{
						flag++;
						
						//System.out.println("play flag="+flag);
						
						synchronized(CrawlerThread.urlList)
						{
							CrawlerThread.urlList.add(id+"@"+url+"@"+category+"@"+kind+"@"+infoId+"@"+showtype+"@"+flag);
						}		
					}
					else 
					{
						jdbc.log("师玉龙", "56+"+id+infoId, 1, "56", url, "no content", 1);
						break;
					}
					break;
				}
				try 
				{
					Thread.sleep(100);
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
		return content;
	}
	
	public static String visitUrlDynamic(String url,String URL)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		
		while(true) 
		{
			try 
			{
				Document doc=Jsoup.connect(url).timeout(3000).userAgent("Mozilla/5.0 (Windows NT 6.1;WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
				content=doc.toString();
				content=content.replaceAll("\n","");
				break;
			} 
			catch (IOException e1) 
			{
				//e1.printStackTrace();
				count++;
				
				//System.out.println("dynamic count="+count);
				
				if (count == 30) //总是出现403错误,多次连接，或增加超时连接时间,若还是无内容，这进行异常处理。
				{
					break;
				}
				try 
				{
					Thread.sleep(100);
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}	
		}  
		return content;
	}
	
	public static String visitUrl2(String id,String url,String category,String kind,String infoId)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		while (true) 
		{
			content = ConnectioinFuction.readURL(url);//获取url的内容content;用httpClient或Jsoup实现；
			if (content != null && !content.equals(""))//content对象非空，且里面的内容非空，即有内容，则退出循环；
				break;
			try {
				Thread.sleep(50);//线程休眠，减少403错误
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
			if (count == 3) //总是出现403错误,多次连接，或增加超时连接时间,若还是无内容，这进行异常处理。
			{
				break;
			}
		}
		return content;
	}
	
	public static String visitUrl3(String id,String url,String category,String kind)
	{
		String content = null;
		int num=0;
		while(num<15)
		{
			try 
			{
				Document doc=Jsoup.connect(url).timeout(3000).userAgent("Mozilla/5.0 (Windows NT 6.1;WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
				content=doc.toString();
				break;
			} 
			catch (IOException e)
			{
				num++;
				if(num>=15)
				{
				}
				try
				{
					Thread.sleep(200);
				}
				catch(InterruptedException e1)
				{
					e1.printStackTrace();
				}
			}	
		}
		return content;
	}
	
	public static String visitUrl4(String id,String url,String category,String kind,String infoId)
	{
		String content = null;
		
		int num=0;
		while(num<15)
		{
			try 
			{
				Document doc=Jsoup.connect(url).timeout(3000).userAgent("Mozilla/5.0 (Windows NT 6.1;WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get();
				content=doc.toString();
				break;
			} 
			catch (IOException e)
			{
				num++;
				if(num>=15)
				{
				}
				try
				{
					Thread.sleep(200);
				}
				catch(InterruptedException e1)
				{
					e1.printStackTrace();
				}
			}	
		}
		return content;
	}
	
	
	public static String visitUrlDyn(String href)  //访问url并获取url的内容content
	{
		String content = null;
		int count = 0;
		while (true) 
		{
			content = ConnectioinFuction.readURL(href);//获取url的内容content;用httpClient或Jsoup实现；
			if (content != null && !content.equals(""))//content对象非空，且里面的内容非空，即有内容，则退出循环；
				break;
			try {
				Thread.sleep(50);//线程休眠，减少403错误
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
			if (count == 20) //总是出现403错误,多次连接，或增加超时连接时间,若还是无内容，这进行异常处理。
			{
				break;
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
		
		//System.out.println("strFind indexStart="+indexStart);
		
		indexEnd=str.indexOf(tagEnd,indexStart);
		if(indexStart>=0&&indexEnd>=0)
		subString=str.substring(indexStart, indexEnd);
		return subString;
	}
	
	public int storeListInfo(String id,String url,String category,String kind,String showtype,long time,int flag,JDBCConnection jdbc)
	{
		//String id;
		String content=null;
		String store=null;   
		StringBuffer sb=new StringBuffer();
		String url1=null;
		String content1=null;
		//id=url.substring(url.lastIndexOf("/")+1,url.lastIndexOf("."));
		
//		System.out.println("store flag="+flag);
//		Date date1=new Date();
//		String dat1=date1.toString().replaceAll(" ","_");
//		System.out.println("dat1="+dat1);
		
		//content=visitUrl(id,url,category,kind);
		content=visitUrlInfo(id,url,category,kind,showtype,flag,jdbc);
		
//		System.out.println("store content="+content);
		url1="http://so.56.com/api_s/operaNew.php?&mid="+id;
		
		
		content1=visitUrlDynamic(url1,url);
		if (content != null && !content.equals(""))
		{
			
//			store=sb.append("56@").append(id).append("@").append(url).append("@").append(category)
//					.append("@Info@").append(showtype).append("@").append(time).append("@").append(System.currentTimeMillis()).append("\n")
//					.append(content).append("*@@@*").append(content1).append("\n").toString();
//			
			String line1="56 56@"+id+"@"+url+"@"+category+"@Info@"+showtype+"@"+time+"@"+System.currentTimeMillis();
			String line2=content+"*@@@*"+content1;
			CrawlerThread.saveData(line1,line2);
			return 1;
		
		}
		
		return -1;
	}
		
	public int storeListPlay(String id,String url,String category,String kind,String infoId,String showtype,long time,int flag,JDBCConnection jdbc)
	{
		//String id;
		String content;
		String store;
		String[] urlDyn=new String[4];//动态数据的url
		String[] contentDyn=new String[4];//contentDyn[0]为动态数据：播放数和顶踩数的内容。[1]为引用数和引用详情的内容
		String vid; 
		//id=url.substring(url.lastIndexOf("_")+1,url.lastIndexOf("."));//有url不存在的情况，进行异常处理
		StringBuffer sb=new StringBuffer();
		//content=visitUrl2(id,url,category,kind,infoId);
		//content=visitUrl4(id,url,category,kind,infoId);
		content=visitUrlPlay(id,url,category,kind,infoId,showtype,flag,jdbc);
		if (content != null && !content.equals(""))
		{
			
			//动态数据
			urlDyn[0]="http://vv.56.com/vv/?id="+id;//播放数和顶踩数url
			contentDyn[0]=visitUrlDynamic(urlDyn[0],url);
			
			urlDyn[1]="http://comment.56.com/trickle/api/commentApi.php?a=flvLatest&vid="+id+"&pct=1&page=1&limit=20";
			contentDyn[1]=visitUrlDynamic(urlDyn[1],url);//  评论数
			
			vid=Share.strFind(content,"\"id\":",",");
			urlDyn[2]="http://www.56.com/quote/v_"+vid+".phtml?";//引用数	
			contentDyn[2]=visitUrlDynamic(urlDyn[2],url);//若引用数为0，则内容为false！！！
	
			urlDyn[3]="http://guess.56.com/like?vids="+vid+"&callback=jsonp_guessLike";
			contentDyn[3]=visitUrlDynamic(urlDyn[3],url);//猜你喜欢
			
//			store=sb.append("56@").append(id).append("@").append(url).append("@").append(category)
//					.append("@Play@").append(showtype).append("@").append(time).append("@").append(System.currentTimeMillis()).append("@").append(infoId).append("\n")
//					.append(content).append("*@@@*").append(contentDyn[0]).append("*@@@*").append(contentDyn[1]).append("*@@@*").append(contentDyn[2]).append("*@@@*").append(contentDyn[3]).append("\n").toString();
//			
			String line1="56 56@"+id+"@"+url+"@"+category+"@Play@"+showtype+"@"+time+"@"+System.currentTimeMillis()+"@"+infoId;
			String line2=content+"*@@@*"+contentDyn[0]+"*@@@*"+contentDyn[1]+"*@@@*"+contentDyn[2]+"*@@@*"+contentDyn[3];
			CrawlerThread.saveData(line1,line2);
			return 1;

		}
	
		return -1;
	}	
}
