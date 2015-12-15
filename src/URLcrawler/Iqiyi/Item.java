package URLcrawler.Iqiyi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Item {
	public static	List DmIntableList = new ArrayList(); 
	public static	List MvHtableList = new ArrayList();
	 public static	List MvtableList = new ArrayList();
	 public static	List TvIntableList = new ArrayList();
	 public static List ZyIntableList=new ArrayList();
	public static int threadCount = 30;
	
	public static void DmInm()	{
		try {
		
			UrlMake urlmaker=new UrlMake();
 		    urlmaker.DmInurlconstructor();
			HashMap allInterURL=urlmaker.getDmInurlMap();
			Iterator itr=allInterURL.entrySet().iterator();
			while(itr.hasNext()) {
				Map.Entry ent=(Map.Entry)itr.next();
				DmIn((String)ent.getValue());
			} 
			 HashSet hs = new HashSet(DmIntableList);  
			 int length=hs.size();
		    Iterator i = hs.iterator();  
		    ArrayList<String> tmpurl=new ArrayList<String>();
		        while(i.hasNext()){  
		          Object temp = i.next();  //每个动漫的url不重复。  
		           String url="Iqiyi DmIn"+temp.toString();
		             tmpurl.add(url);
		        }
	      ArrayList<IqiyiThread> pool = new ArrayList<IqiyiThread>();
			Date d=new Date();
			IqiyiThread.urlList =tmpurl;
			for(int j = 0;j < threadCount;j++) {
				  IqiyiThread downthread = new IqiyiThread(d.getTime(),d.toString(),1,pool);
				
					pool.add(downthread);
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					new Thread(downthread).start();
				}
			 int count=1;
				while(true) {
					try {
						Thread.sleep(60000);
					} catch(InterruptedException e) {
						e.printStackTrace();
					}
					synchronized(pool) {
						if(pool.size()<=0) {
							break;
						}
					}
					count++;
					if(count==30) {
						break;
					}
				}
				pool=null;		          
		     
		} catch(Exception e) {
		
		}
	}
	public static void  DmIn(String interurl) throws IOException, InterruptedException  {
		String baseurl="http://list.iqiyi.com/www/4/";
	    String strurl=baseurl+interurl; 
		UrlOut oputfile=new UrlOut();
		oputfile.DmInoutput(strurl);
	}
	public static void MvHm() {
		try {
			  UrlMake urlmaker=new UrlMake();
 			 urlmaker.MvHurlconstructor();
			HashMap allInterURL=urlmaker.getMvhurlMap();
			Iterator itr=allInterURL.entrySet().iterator();
			while(itr.hasNext()) {
				Map.Entry ent=(Map.Entry)itr.next();
				Mvh((String)ent.getValue());
			} 
			 HashSet hs = new HashSet(MvHtableList);  
			     int length=hs.size();
		        Iterator i = hs.iterator(); 
		        ArrayList<String> tmpurl=new ArrayList<String>();
		        while(i.hasNext()){  
		               Object temp = i.next();  //每个电影的url不重复。  
		             String url="Iqiyi MvPh"+temp.toString();
		             tmpurl.add(url);
		        }
		      ArrayList<IqiyiThread> pool = new ArrayList<IqiyiThread>();
				Date d=new Date();
				IqiyiThread.urlList =tmpurl;
				for(int j = 0;j < threadCount;j++) {
					  IqiyiThread downthread = new IqiyiThread(d.getTime(),d.toString(),1,pool);
						
						pool.add(downthread);
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						new Thread(downthread).start();
					}
				int count=1;
				while(true) {
					try {
						Thread.sleep(60000);
					} catch(InterruptedException e) {
						e.printStackTrace();
					}
					synchronized(pool) {
						if(pool.size()<=0) {
							break;
						}
					}
					count++;
					if(count==30) {
						break;
					}
				}
				pool=null;
		} catch(Exception e) {
		}
	}
	public static void  Mvh(String interurl) throws IOException, InterruptedException  {
		String baseurl = "http://list.iqiyi.com/www/10/";
	    String strurl=baseurl+interurl; 
		UrlOut oputfile=new UrlOut();
		oputfile.MvHoutput(strurl);	
	}	

		public static void Moviem()	{
			try {
				  UrlMake urlmaker=new UrlMake();
	 			 urlmaker.Mvurlconstructor();
				HashMap allInterURL=urlmaker.getMvurlMap();
				Iterator itr=allInterURL.entrySet().iterator();
				while(itr.hasNext()) {
					Map.Entry ent=(Map.Entry)itr.next();
					movie((String)ent.getValue());
				} 
				 HashSet hs = new HashSet(MvtableList);  
				     int length=hs.size();
			        Iterator i = hs.iterator();  
			   	String[] colfams = {"C"};
			   	String[] quals = {"url"};
			      String[] values={""};
			     ArrayList<String> tmpurl=new ArrayList<String>();
			       while(i.hasNext()){  
			               Object temp = i.next();  //每个电影的url不重复。  
			               String url="Iqiyi MvIn"+temp.toString();
			               if((url.contains("/lib/")||url.contains("/ceshi/"))==true) {
			            	   continue;
			               }
			               else {
			            	      tmpurl.add(url);
			                }                            
			        }//while
			       
			   ArrayList<IqiyiThread> pool = new ArrayList<IqiyiThread>();
				Date d=new Date();
				IqiyiThread.urlList =tmpurl;
				for(int j = 0;j < threadCount;j++) {
				  IqiyiThread downthread = new IqiyiThread(d.getTime(),d.toString(),1,pool);
					
					pool.add(downthread);
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					new Thread(downthread).start();
				}
				int count=1;
				while(true) {
					try {
						Thread.sleep(60000);
					} catch(InterruptedException e) {
						e.printStackTrace();
					}
					synchronized(pool) {
						if(pool.size()<=0) {
							break;
						}
					}
					count++;
					if(count==30) {
						break;
					}
				}
				pool=null;		          
			} catch(Exception e) {
			}
		}
		public static void  movie(String interurl) throws IOException, InterruptedException  {
			String baseurl="http://list.iqiyi.com/www/1/";
		    String strurl=baseurl+interurl; 
			UrlOut oputfile=new UrlOut();
			oputfile.Mvoutput(strurl);	
		}
		public static void TvInm()	{
			try {
			   UrlMake urlmaker=new UrlMake();
	 		    urlmaker.TvInurlconstructor();
				HashMap allInterURL=urlmaker.getTvInurlMap();
				Iterator itr=allInterURL.entrySet().iterator();
				while(itr.hasNext()) {
					Map.Entry ent=(Map.Entry)itr.next();
					TvIn((String)ent.getValue());
				} 
				 HashSet hs = new HashSet(TvIntableList);  
				     int length=hs.size();
			        Iterator i = hs.iterator();  
				      ArrayList<String> tmpurl=new ArrayList<String>();
			        while(i.hasNext()){  
			               Object temp = i.next();  //每个电视剧首页的url不重复。  
			               String url="Iqiyi TvIn"+temp.toString();
			              tmpurl.add(url);                     
			        }
			      ArrayList<IqiyiThread> pool = new ArrayList<IqiyiThread>();
					Date d=new Date();
					IqiyiThread.urlList =tmpurl;
					for(int j = 0;j < threadCount;j++) {
						  IqiyiThread downthread = new IqiyiThread(d.getTime(),d.toString(),1,pool);
							
							pool.add(downthread);
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							new Thread(downthread).start();
						}
					int count=1;
						while(true) {
							try {
								Thread.sleep(60000);
							} catch(InterruptedException e) {
								e.printStackTrace();
							}
							synchronized(pool) {
								if(pool.size()<=0) {
									break;
								}
							}
							count++;
							if(count==30) {
								break;
							}
						}
						pool=null;		          
			} catch(Exception e) {
				e.printStackTrace();
			
			}
		}
		public static void  TvIn(String interurl) throws IOException, InterruptedException  {
			String baseurl="http://list.iqiyi.com/www/2/";
		    String strurl=baseurl+interurl; 
			UrlOut oputfile=new UrlOut();
			oputfile.TvInoutput(strurl);
		}
		public static void ZyInm() throws IOException, InterruptedException	{
			  UrlMake urlmaker=new UrlMake();
	 		  urlmaker.ZyInurlconstructor();
				HashMap allInterURL=urlmaker.getZyInurlMap();
				Iterator itr=allInterURL.entrySet().iterator();
				while(itr.hasNext()) {
					Map.Entry ent=(Map.Entry)itr.next();
					ZyIn((String)ent.getValue());
				} 
				 HashSet hs = new HashSet(ZyIntableList);  
			     int length=hs.size();
		        Iterator i = hs.iterator();  
			      ArrayList<String> tmpurl=new ArrayList<String>();
		        while(i.hasNext()){  
		               Object temp = i.next();  //每个电视剧首页的url不重复。  
		               String url="Iqiyi ZyIn"+temp.toString();
		              tmpurl.add(url);                     
		        }
		      ArrayList<IqiyiThread> pool = new ArrayList<IqiyiThread>();
				Date d=new Date();
				IqiyiThread.urlList =tmpurl;
				for(int j = 0;j < threadCount;j++) {
					  IqiyiThread downthread = new IqiyiThread(d.getTime(),d.toString(),1,pool);	
						pool.add(downthread);
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						new Thread(downthread).start();
					}
				int count=1;
					while(true) {
						try {
							Thread.sleep(60000);
						} catch(InterruptedException e) {
							e.printStackTrace();
						}
						synchronized(pool) {
							if(pool.size()<=0) {
								break;
							}
						}
						count++;
						if(count==120) {
							break;
						}
					}
					pool=null;		          
		}
		
public static void  ZyIn(String interurl) throws IOException, InterruptedException  {
	String baseurl="http://list.iqiyi.com/www/6/-";
    String strurl=baseurl+interurl; 
    UrlOut oputfile=new UrlOut();
	oputfile.ZyInoutput(strurl);
}

}
