package URLcrawler.URLcrawler_56;

import java.util.ArrayList;

public class CrawlerThread implements Runnable
{
	public long time;
	public static String date;
	public static ArrayList<CrawlerThread> pool;//存储多个线程
	public static ArrayList<String> urlListInfo=new ArrayList<String>();
	public static ArrayList<String> urlListPlay=new ArrayList<String>();
	public static ArrayList<String> urlList=new ArrayList<String>();
	//public static ArrayList<String> urlListPosition=new ArrayList<String>();
	
	public CrawlerThread(long time,String date,ArrayList<CrawlerThread> pool)
	{
		this.time=time;
		CrawlerThread.date=date;
		CrawlerThread.pool=pool;
	}
	
	@Override
	public void run()//实现run()方法
	{
		while(true)
		{
			String item = null;
			String id= null;
			String url=null;
			String category= null;
			String showtype=null;
			String[] str1= null;
			String[] str2= null;
			String[] str = null;
			synchronized(urlListInfo) //同步urlList对象，加锁,互斥访问共享资源
			{
				if(urlListInfo!=null&&urlListInfo.size()>0)
				{
					item=urlListInfo.get(0);
					urlListInfo.remove(0);
				}
				else if(urlListInfo==null||urlListInfo.size()==0)
				{
					synchronized(pool)//同步线程池中的线程对象
					{
						pool.remove(this);
					}
					break;
				}
			}
			if(item!=null&&!item.equals(""))
			{
				//System.out.println("item="+item);
//				str1=item.split("\\$");//$为正则表达式的关键字，需进行转义；
//				id=str1[0];
//				str2=str1[1].split("@");
//				url=str2[0];
//				category=str2[1];
				str=item.split("@");
				id=str[0];
				url=str[1];
				category=str[2];
				showtype=str[4];
				
//				System.out.println("id="+id);
//				System.out.println("url="+url);
//				System.out.println("category="+category);
				//System.out.println("showtype="+showtype);
				
				if(category.equals("Movie")) 
				{
					Share.getPlayUrl(id, url, category, showtype, "[class=play_btn]");//132条
				}
				else if(category.equals("Tv")||category.equals("Cartoon")||category.equals("Show")) 
				{
				    Share.getPlayUrl(id, url, category, showtype, "[class=episode]");
				}
			}	
		}
		
	}
}
