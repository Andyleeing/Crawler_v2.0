package URLcrawler.Iqiyi;

import hbase.HBaseCRUD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.jsoup.Jsoup;
import org.jsoup.helper.HttpConnection;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;

public class IqiyiThread implements Runnable {
	public  HBaseCRUD hbase = new HBaseCRUD();
	public long time;
	public String date;
	public String type;
	static Integer k = 1;
	public String parenturl = null;
	public static ArrayList<IqiyiThread> pool;
	public static ArrayList<String> urlList = null;
  public	JDBCConnection conn= new JDBCConnection();
	int step;
	String[] colfams = { "C" };
	String[] quals = { "url" };
//	String[] values = { "" };
	
	public IqiyiThread(long time, String date, int step,
			ArrayList<IqiyiThread> mypool) {
		this.time = time;
		this.date = date;
		this.step = step;
		pool = mypool;
	}

	public void SaveUrl(String tmpurl) {
		String[] rows = { tmpurl };
		String[] values = { tmpurl };
		try {
			hbase.putRows("iqiyiListAll", rows, colfams, quals, values);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void TvIt(String tmpurl) { // 把电视剧每集的URL放入Iqiyilist表中。（改完）
		 parenturl = tmpurl.substring(10);
		
		Document doc = null;
		try {
			HttpConnection conn = (HttpConnection) Jsoup.connect(parenturl);
			conn.timeout(20000);
			conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
			doc = conn.get();// doc指每个电视剧首页的原网页。
			

			Elements contentPart = null;
			Elements contentPartyong=null;
			contentPartyong=doc.getElementsByAttributeValue("data-widget","albumlist-render");
			if(contentPartyong==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("李辉", parenturl+"+iy", 1, "iy", parenturl, "此URL列表未能获得", 3);
				logc.closeConn();				
			}
			
			Elements allurlyong=contentPartyong.select("p>a[href]");
			for (Element Link : allurlyong) {
				if (Link != null) {
					String temp = "Iqiyi TvIt" + Link.attr("href") + "^"
							+ parenturl ;
					SaveUrl(temp);
				}
			} 

			contentPart = doc.getElementsByAttributeValue("class",
					"dongman_jujiBlock jujiBlock");
			String content = contentPart.toString();
			int indexsta = content.indexOf(" style=\"display: none;\">");
			if (indexsta >= 0) {    //如风中奇缘那样的的电视剧。
				int indexst = 0;
				int endst = 0;
				while (indexst >= 0) {
					indexst = content.indexOf("none;\">", endst);
					if (indexst >= 0) {
						endst = content.indexOf("</div>", indexst);
						String Iturl = content.substring(indexst + 7, endst);
						String iturl = "http://www.iqiyi.com"
								+ Iturl.replaceAll("\\s*", "");
						HttpConnection conn1 = (HttpConnection) Jsoup
								.connect(iturl);
						conn1.timeout(10000);
						conn1.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
						Document doc1 = conn1.get();// doc指每个电视剧首页的原网页。
						Elements allLink = null;
						Elements contentPa = null;
						contentPa = doc1.getElementsByAttributeValue("class",
								"list_block1 align_c");
						allLink = contentPa.select("p>a[href]");
						if(allLink==null) {
							JDBCConnection logc = new JDBCConnection();
							logc.log("李辉", iturl+"+iy", 1, "iy", iturl, "此URL列表未能获得", 3);
							logc.closeConn();				
						}
						
						for (Element Link : allLink) {
							if (Link != null) {
								String temp = "Iqiyi TvIt" + Link.attr("href") + "^"
										+ parenturl  ;
								SaveUrl(temp);
							}
						}

					}
				}
			}  
			
		} catch (IOException e) {
		//	e.printStackTrace();
		}
	}

	
	
	public void DmIt(String tmpurl) {   //（改完）
		 parenturl = tmpurl.substring(10);
		
		Document doc = null;
		try {
			HttpConnection conn = (HttpConnection) Jsoup.connect(parenturl);
			conn.timeout(20000);
			conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
			doc = conn.get();// doc指每个动漫首页的原网页。
			String contenta = doc.toString();
			int paidst = contenta.indexOf("albumId:");
			
			Elements contentPartsample=null;
			contentPartsample=doc.getElementsByAttributeValue("data-widget","albumlist-render");
			if(contentPartsample==null) {
				JDBCConnection logc = new JDBCConnection();
				logc.log("李辉", parenturl+"+iy", 1, "iy", parenturl, "此URL列表未能获得", 3);
				logc.closeConn();				
			}
			
			Elements allurlyong=contentPartsample.select("p>a[href]");
		
			for (Element Link : allurlyong) {
				if (Link != null) {
					String temp = "Iqiyi DmIt" + Link.attr("href") + "^"
							+ parenturl;
					SaveUrl(temp);
				}
			}  
	
			Elements contentPart = null;
			contentPart = doc.getElementsByAttributeValue("class",
					"dongman_jujiBlock jujiBlock");
			String content = contentPart.toString();
			int indexsta = content.indexOf(" style=\"display: none;\">");
			if (indexsta >= 0) {       
				int indexst = 0;
				int endst = 0;
				while (indexst >= 0) {
					indexst = content.indexOf("none;\">", endst);
					if (indexst >= 0) {
						endst = content.indexOf("</div>", indexst);
						String Iturl = content.substring(indexst + 7, endst);
						String iturl = "http://www.iqiyi.com"
								+ Iturl.replaceAll("\\s*", "");
						HttpConnection conn1 = (HttpConnection) Jsoup
								.connect(iturl);
						conn1.timeout(100000);
						conn1.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");
						Document doc1 = conn1.get();// doc指每个动漫首页的原网页。
						Elements allLink = null;
						Elements contentPa = null;
						contentPa = doc1.getElementsByAttributeValue("class",
								"list_block1 align_c");
						if(contentPa==null) {
							JDBCConnection logc = new JDBCConnection();
							logc.log("李辉", iturl+"+iy", 1, "iy", iturl, "此URL列表未能获得", 3);
							logc.closeConn();				
						}
						
						allLink = contentPa.select("p>a[href]");
						for (Element Link : allLink) {
							if (Link != null) {
								String temp = "Iqiyi DmIt" + Link.attr("href") + "^"
										+ parenturl ;
								SaveUrl(temp);
							}

						}
					}
				}
			}
		} catch (IOException e) {
		//	e.printStackTrace();
		}
	}
	
	
	
	public void ZyIt(String tmpurl) {
		 parenturl = tmpurl.substring(10);  //综艺节目首页的URL
        try {
 	if(!parenturl.contains("/lib")) {      //包含lib关键字的info,如变形计。
     	                                                   //处理如康熙来了的节目。
     	 Document doc=null;
       	 HttpConnection conn=(HttpConnection) Jsoup.connect(parenturl);
         conn.timeout(10000);
     	 conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0"); 
         doc =conn.get();//doc指每个综艺节目首页的原网页。
           String content=doc.toString();
           int idindex=content.indexOf("sourceId:");
           if(idindex>=0) {   //>=0,只能说明它满足康熙来了这样的节目。其他稍后处理。
         	  
         	  int idend=content.indexOf(",",idindex);
         	    String id=content.substring(idindex+9,idend).replaceAll("\\s*",""); //得到ID。
         	     String year[]={"2015","2014","2013","2012","2011","2010","2009","2008","2007","2006","2005","2004","2003","2002","2001","2000","1999"};
         	     String temp_year=null,temp_url=null;

         	          for(int i=0;i<year.length;i++) {
         	        	    temp_year=year[i];
         	        	  temp_url="http://cache.video.qiyi.com/sdvlst/6/"+id+"/"+temp_year+"/?cb=scDtVdListC";//得到拥有临时年份的节目列表。
         	        	  Document docin=null;
			              	 HttpConnection connin=(HttpConnection) Jsoup.connect(temp_url);
			                connin.timeout(10000);
			            	 connin.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0"); 
			                docin =connin.get();//doc指每个综艺节目首页的原网页。
			            	if(docin==null) {
			    				JDBCConnection logc = new JDBCConnection();
			    				logc.log("李辉", temp_url+"+iy", 1, "iy", temp_url, "此URL列表未能获得", 3);
			    				logc.closeConn();				
			    			}
			                
			                
			                String contentin=docin.toString();
			                int indexst=0,indexed=0;
			                    while(indexst>=0) {
			                    	  indexst=contentin.indexOf("vUrl&quot;:&quot;",indexed);
			                    	     if(indexst>=0) {
			                    	    	 indexed=contentin.indexOf("&quot;,",indexst);
			                    	    	 String jiurl="Iqiyi ZyIt"+contentin.substring(indexst+17,indexed)+"^"+parenturl;//用^和父类ID建立关联。
			                    	    		SaveUrl(jiurl);  //temp_url能够保证URL不重复.(即使该年内容为空).
			                    	           
			                    	     }
			                    }
         	          }
           }
     	 
      }//不包含lib.
      else {    //包含lib.
     	 Document doc=null;
       	 HttpConnection conn=(HttpConnection) Jsoup.connect(parenturl);
         conn.timeout(100000);
     	 conn.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0"); 
         doc =conn.get();//doc指每个综艺节目首页的原网页。
     	if(doc==null) {
			JDBCConnection logc = new JDBCConnection();
			logc.log("李辉", parenturl+"+iy", 1, "iy", parenturl, "此URL列表未能获得", 3);
			logc.closeConn();				
		}
        
         String libcontent=doc.toString();
         int    indexid=libcontent.indexOf("data-doc-id=\"");
            if(indexid>=0) {
         	      int indexed=libcontent.indexOf("\"",indexid+13);
         	      String id=libcontent.substring(indexid+13,indexed);  //节目不同ID就不同。
         	            int indexid1=libcontent.indexOf("data-upanddown-albumid=\"");
         	            String id1=null;
         	            if(indexid1>=0) {
         	            	 int indexed1=libcontent.indexOf("\"",indexid1+25);
         	            	 id1=libcontent.substring(indexid1+24,indexed1);
         	             }
         	           for(int page=1;page<20;page++) {    
         	        	   String temp_url="http://rq.video.iqiyi.com/aries/e.jsonp?site=iqiyi&docId="+id+"&from="+page+"&count=20&cb=cb_movsource";//非常了得的info地址。
         	        	   StringBuffer content=new StringBuffer();
         	        	   URL get=new URL(temp_url);
             	            HttpURLConnection connection=(HttpURLConnection)get.openConnection();
				                        connection.connect();
	 				                    BufferedReader reader=new BufferedReader(new InputStreamReader(connection.getInputStream()));
	 				                       String lines=null;
	 				                       while((lines=reader.readLine())!=null) {
	 				                    	     content.append(lines);
	 				                       }
	 				 
				                String contentin=content.toString();
         	        	       int indexst=0,indexedd=0;
			                    while(indexst>=0) {//每页的URL.
			                    	  indexst=contentin.indexOf("play_url\":\"",indexedd);
			                    	     if(indexst>=0) {
			                    	    	 indexedd=contentin.indexOf("\",",indexst);
			                    	    	 String jiurl="Iqiyi ZyIt"+contentin.substring(indexst+11,indexedd)+"^"+parenturl;//用^和父类ID建立关联。
							            	SaveUrl(jiurl);
			                    	   // 	out.write(jiurl.getBytes());
			                    	     }
         	           }
            }//for
         	        
   		        }
      }
        } catch(Exception e) {
     	   //   e.printStackTrace();
        }
		
	}

	@Override
	public void run() {
		while (true) {
			String url = "";
			synchronized (urlList) {
				if (urlList != null && urlList.size() > 0) {
					url = urlList.get(0);
					urlList.remove(0);
				} else if (urlList == null || urlList.size() <= 0) {
					synchronized (pool) {
						pool.remove(this);
						break;
					}
				}
			}

			if (step == 1) {
				if (url.contains("TvIn")) { // 存储电视剧每集的URL
					SaveUrl(url); 
					TvIt(url);
				}
				if (url.contains("DmIn")) { // 存储动漫每集的URL
					SaveUrl(url); 
					DmIt(url);
				}
				if(url.contains("ZyIn")) {
			   SaveUrl(url); 
				ZyIt(url);
				}
				if (url.contains("MvIn") || url.contains("MvPh")) {
					SaveUrl(url); // 电影，片花，电视剧或动漫的Info页直接保存。
				}
			
			}
		}
	}
}
