package URLcrawler.URLcrawler_56;

import hbase.HBaseCRUD;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import Utils.JDBCConnection;
import Utils.SysParams;
import jxHan.Crawler.Util.Log.ExceptionHandler;

public class CrawlerUrl 
{
	public static void main56()
	//public static void main(String[] args) 
	{
		//每次循环抓取前，urlList清空，防止url累加存储.使url.txt非法增大
		CrawlerThread.urlListInfo.clear();
		CrawlerThread.urlListPlay.clear();
		CrawlerThread.urlList.clear();

		Date date=new Date();
		String dat=date.toString().replaceAll(" ","_");
		System.out.println("datS="+dat);

		String content="抓取url开始，info="+(CrawlerThread.urlList.size()-CrawlerThread.urlListPlay.size())+",play="+CrawlerThread.urlListPlay.size();
		String sql="insert into Log (machine,level,time,content,website,manager) values('" + SysParams.urlTable_Hbase_local + "',"+1+","+"now()"+",'"+content+"','56','师玉龙');";
		JDBCConnection jdbccon = new JDBCConnection();
		jdbccon.update(sql);
		jdbccon.closeConn();
		get56Url(date);
		// 复制urlListPlay到urlList;
		CrawlerThread.urlList.addAll(CrawlerThread.urlListPlay);
		
	
		int lenUrlList=CrawlerThread.urlList.size();
		//System.out.println("urlList size="+lenUrlList);

		//hbase
		HBaseCRUD hbase=new HBaseCRUD();
		String item=null;
		String[]colfams={"C"};
		String[]quals={"url"};
		String[]splits=null;
		for(int i=0;i<lenUrlList;i++)
		{
			item=CrawlerThread.urlList.get(i);
			item="56 "+item;
			splits=item.split("@");
			String[] rows={splits[0]};
			String[] values={item};
			try 
			{
				hbase.putRows("url56new", rows, colfams, quals, values);
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
		Date dateF=new Date();
		String datF=dateF.toString().replaceAll(" ","_");
	
		
		content="抓取url结束，info="+(CrawlerThread.urlList.size()-CrawlerThread.urlListPlay.size())+",play="+CrawlerThread.urlListPlay.size();
		sql="insert into Log (machine,level,time,content,website,manager) values('" + SysParams.urlTable_Hbase_local + "',"+1+","+"now()"+",'"+content+"','56','师玉龙');";
		JDBCConnection jdbccon1 = new JDBCConnection();
		jdbccon1.update(sql);
		jdbccon1.closeConn();

	}
	
	private static void get56Url(Date date) 
	{
		ArrayList<CrawlerThread> pool=new ArrayList<CrawlerThread>();//存储线程数组列表
		
		//获取infoUrl
		try 
		{
			Urls(date);
		} 
		catch (IOException e1) 
		{
			e1.printStackTrace();
		}
		
		//playUrl,创建并启动线程
		int threadCount=2;
		//int threadCount=1;
		for(int i=0;i<threadCount;i++)
		{
			CrawlerThread crawlerThread=new CrawlerThread(date.getTime(),date.toString(),pool);
			new Thread(crawlerThread).start();//创建无名对象，启动线程
			pool.add(crawlerThread);
			try
			{
				Thread.sleep(200);
			}    
			catch(InterruptedException e)
			{
				e.printStackTrace();
			}	
		}	
		
		while(true)
		{
			synchronized(pool)
			{
				if(pool.size()<=0) break;
			}
			try
			{
				Thread.sleep(60000);
			}
			catch(InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		System.out.println("pool end");
	}
	
	private static void Urls(Date date) throws IOException
	{
		//pageUrl
		ArrayList<String> pageUrlMovie=new ArrayList<String>();//存储每页的url
		ArrayList<String> pageUrlTv=new ArrayList<String>();
		ArrayList<String> pageUrlCartoon=new ArrayList<String>();
		ArrayList<String> pageUrlShow=new ArrayList<String>();
		
		pageUrlMovie=Share.getPageUrl("http://video.56.com/dy/", "http://video.56.com/tv-v-movie_sort-n_tid-_zid-_yid-_page-", pageUrlMovie,"正片");//正片116
		System.out.println("page正片标记="+pageUrlMovie.size());
		pageUrlMovie=Share.getPageUrl("http://video.56.com/tv-v-jvideo_sort-n_tid-_zid-_yid-_page-1.html", "http://video.56.com/tv-v-jvideo_sort-n_tid-_zid-_yid-_page-", pageUrlMovie,"预告片");//预告片633
		System.out.println("page预告片标记="+pageUrlMovie.size());
		pageUrlMovie=Share.getPageUrl("http://video.56.com/videolist-c-25_sort-rela_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-25_sort-rela_charset-utf-8_page-", pageUrlMovie,"花絮");//花絮1200
		System.out.println("page花絮标记="+pageUrlMovie.size());
		pageUrlMovie=Share.getPageUrl("http://video.56.com/videolist-c-25_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-25_charset-utf-8_page-", pageUrlMovie,"视频");//视频334
		System.out.println("page视频标记="+pageUrlMovie.size());
		
		pageUrlTv=Share.getPageUrl("http://video.56.com/dsj/", "http://video.56.com/tv-v-tv_sort-n_tid-_zid-_yid-_page-", pageUrlTv,"正片");//257		
		pageUrlTv=Share.getPageUrl("http://video.56.com/videolist-c-35_sort-rela_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-35_sort-rela_charset-utf-8_page-", pageUrlTv,"花絮");//1200
		pageUrlTv=Share.getPageUrl("http://video.56.com/videolist-c-35_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-35_charset-utf-8_page-", pageUrlTv,"视频");//357
		
		pageUrlCartoon=Share.getPageUrl("http://video.56.com/dm/", "http://video.56.com/tv-v-dm_sort-n_tid-_zid-_yid-_page-", pageUrlCartoon,"正片");//191
		pageUrlCartoon=Share.getPageUrl("http://video.56.com/videolist-c-8_sort-rela_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-8_sort-rela_charset-utf-8_page-", pageUrlCartoon,"花絮");//35
		pageUrlCartoon=Share.getPageUrl("http://video.56.com/videolist-c-8_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-8_charset-utf-8_page-", pageUrlCartoon,"视频");//203
		
		pageUrlShow=Share.getPageUrl("http://video.56.com/zy/", "http://video.56.com/tv-v-zy_sort-n_tid-_zid-_yid-_page-", pageUrlShow,"正片");//546
		pageUrlShow=Share.getPageUrl("http://video.56.com/videolist-c-45_sort-rela_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-45_sort-rela_charset-utf-8_page-", pageUrlShow,"花絮");//1200
		pageUrlShow=Share.getPageUrl("http://video.56.com/videolist-c-45_charset-utf-8_page-1.html", "http://video.56.com/videolist-c-45_charset-utf-8_page-", pageUrlShow,"视频");//397
		//infoUrl
		int count1=0;
		int count2=0;
		int count3=0;
		int count4=0;
		ArrayList<String> urlInfo=new ArrayList<String>();//存储信息页的url,共575条
		//System.out.println("InfoSize Movie="+urlInfo.size());
		urlInfo=Share.getInfoUrl(pageUrlMovie,urlInfo,"Movie",date);
		count1=urlInfo.size();
		System.out.println("InfoSize Movie="+count1);//2283=116(正片info)+633(预告片play)+1200(花絮play)+334(视频play)
		urlInfo=Share.getInfoUrl(pageUrlTv,urlInfo,"Tv",date);//280条
		count2=urlInfo.size();
		System.out.println("InfoSize Tv="+(count2-count1));
		urlInfo=Share.getInfoUrl(pageUrlCartoon,urlInfo,"Cartoon",date);//178条
		count3=urlInfo.size();
		System.out.println("InfoSize Cartoon="+(count3-count2));
		urlInfo=Share.getInfoUrl(pageUrlShow,urlInfo,"Show",date);//546条
		count4=urlInfo.size();
		System.out.println("InfoSize Show="+(count4-count3));
			
		CrawlerThread.urlListInfo=urlInfo;//复制urlInfo到urlListInfo,直接复制地址
		CrawlerThread.urlList.addAll(urlInfo);//复制urlInfo到urlList
		
		int lenInfo=CrawlerThread.urlListInfo.size();

		int lenPlay=CrawlerThread.urlListPlay.size();

	}	
}
