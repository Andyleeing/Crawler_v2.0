package DataCrawler.Iqiyi;

import hbase.HBaseCRUD;

import java.io.IOException;

import Utils.JDBCConnection;
import DataCrawler.CrawlerThread;
	

public class MovieFuction {
	public static int i = 100;
	public HBaseCRUD hbase = new HBaseCRUD();
	
	public int infoCrawler(String tmpurl,long time,String date,String crawltime,JDBCConnection jdbc) {
		int page=1;
		int ypage=1;
		int flag=-1;
		StringBuffer allcontent = new StringBuffer();
		String url=tmpurl.substring(4);
		if (url == null || url.equals(""))
			return flag;
		String movieinfocontent = "";
		String movieincon = null;
		try {
			movieincon = dongmanFunction.visitURL(url);
			if(movieincon !=null) {
			flag=1;
			} else {
				flag=-1;
			}
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}
		try {
			Thread.sleep(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		movieinfocontent="info------------------"+url+movieincon;
		 allcontent.append(movieinfocontent);
		 int idstart=movieinfocontent.indexOf("albumId");
		 if(idstart>=0) {
		 String idcode=movieinfocontent.substring(idstart); 
		 int idst=idcode.indexOf("albumId");
		 int ided=idcode.indexOf(",");
		 String did=idcode.substring(idst,ided);
		 String id=did.replaceAll("\\D*", ""); 
		 int tvstart=movieinfocontent.indexOf("tvId");
		 String tvcode=movieinfocontent.substring(tvstart);
		 int tvst=tvcode.indexOf("tvId");
		 int tved=tvcode.indexOf(",");
		 String ttv=tvcode.substring(tvst,tved);
		 String tv=ttv.replaceAll("\\D*", "");
        
		 
		 int qitanid=movieinfocontent.indexOf("data-qitancomment-qitanid=\"");
		 String qitanidd=null;
		 if(qitanid>=0) {
			 String qitancode=movieinfocontent.substring(qitanid);
			 int    qitanst=qitancode.indexOf("qitanid=\"");
			 int    qitanend=qitancode.indexOf("\"",qitanst+10);
			 qitanidd=qitancode.substring(qitanst+9,qitanend);
		 } else {
		 qitanid=movieinfocontent.indexOf("qitanId\":");
		 if(qitanid>=0) {
			 String qitancode=movieinfocontent.substring(qitanid);
			 int    qitanst=qitancode.indexOf("qitanId=\":");
			 int    qitanend=qitancode.indexOf(",",qitanst);
			 qitanidd=qitancode.substring(qitanst+10,qitanend);
		 }
		 }
			
		 String sumplaycount = "http://cache.video.qiyi.com/jp/pc/" + id
					+ "/?callback=window.Q.__callbacks__.cbgt6rz7";
			String summcode = null;
			try {
				summcode = dongmanFunction.visitURL(sumplaycount);
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			dongmanFunction.Save(allcontent, sumplaycount, "Sumplaycount", url,
					summcode);
		 
		 String commurl="http://api.t.iqiyi.com/qx_api/comment/get_video_comments?aid="+qitanidd+"&categoryid=1&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1"+"&page_size=10&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="+qitanidd+"&sort=hot&t=0.2906751498985618&tvid="+tv;
			String commcontent=null;
			try {
				commcontent = dongmanFunction.visitURL(commurl);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			dongmanFunction.Save(allcontent, commurl, "Comment", url,  commcontent);
			
			String  yurl="http://api.t.iqiyi.com/qx_api/comment/review/get_review_list?aid="+qitanidd+"&categoryid=1&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=5&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="+qitanidd+"&sort=hot&t=0.9215203301954558&tvid="+tv;
			   String ycontent=null;
				try {
					ycontent = dongmanFunction.visitURL(yurl);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			dongmanFunction.Save(allcontent, yurl, "Ycomment", url,  ycontent);
			
			
	
		 String precentge="http://cache.video.qiyi.com/pc/pr/"+id+"/playCountPCMobileCb?callback=playCountPCMobileCb";
		 String preContent = null;
		try {
			preContent = dongmanFunction.visitURL(precentge);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, precentge, "Precentge", url,  preContent);
		 
		 
	 
			 String reference="http://mixer.video.iqiyi.com/jp/recommend/videos?referenceId="+tv+"&albumId="+id+"&channelId=1&cookieId=c15080ccea46c39cb733eec29be0e154&withRefer=true&area=zebra&size=10&type=video&pru=&callback=window.Q.__callbacks__.cbhdjhdv";
			 String refContent=null;
			try {
				refContent = dongmanFunction.visitURL(reference);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
				try {
					Thread.sleep(i);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				dongmanFunction.Save(allcontent, reference, "Reference", url,  refContent);  
		
				 String free="http://serv.vip.iqiyi.com/pay/movieBuy.action?aid="+id+"&cb=__getPayBtn__cb_";
				 String freeContent=null;
				try {
					freeContent = dongmanFunction.visitURL(free);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
					try {
						Thread.sleep(i);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					dongmanFunction.Save(allcontent, free, "Free", url,  freeContent);  
				
				
				String IqiyiRow ="Iqiyi ZhengPian"+id+"#"+time;
			if(allcontent!=null && !allcontent.equals("")) {
				String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
				String line2=allcontent.toString();
				CrawlerThread.saveData(line1, line2);
				if(movieincon==null) {
					jdbc.log("李辉", url+"+iy", 1, "iy", url, "URL访问失败", 1);
				}
			}
			
			  allcontent=null;	
	}
		  return flag;
	}
	
	public int infohCrawler(String tmpurl,long time,String date,String crawltime,JDBCConnection jdbc) {
		int page=1;
		int ypage=1;
	    int flag=-1;
		StringBuffer allcontent = new StringBuffer();
		String url=tmpurl.substring(4);
		if (url == null || url.equals(""))
			return flag;
		String movieinfocontent = "";
		String movieincon = null;
		try {
			movieincon = dongmanFunction.visitURL(url);
			if(movieincon !=null) {
				flag=1;
			} else {
				flag=-1;
			}
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}
		try {
			Thread.sleep(i);
		} catch (Exception e) {
			e.printStackTrace();
		}
		movieinfocontent="info------------------"+url+movieincon;
		 allcontent.append(movieinfocontent);
		 int idstart=movieinfocontent.indexOf("albumId");
		 if(idstart>=0) {
		 String idcode=movieinfocontent.substring(idstart); 
		 int idst=idcode.indexOf("albumId");
		 int ided=idcode.indexOf(",");
		 String did=idcode.substring(idst,ided);
		 String id=did.replaceAll("\\D*", ""); 
		 int tvstart=movieinfocontent.indexOf("tvId");
		 String tvcode=movieinfocontent.substring(tvstart);
		 int tvst=tvcode.indexOf("tvId");
		 int tved=tvcode.indexOf(",");
		 String ttv=tvcode.substring(tvst,tved);
		 String tv=ttv.replaceAll("\\D*", "");
		 
		 int qitanid=movieinfocontent.indexOf("data-qitancomment-qitanid=\"");
		 String qitanidd=null;
		 if(qitanid>=0) {
			 String qitancode=movieinfocontent.substring(qitanid);
			 int    qitanst=qitancode.indexOf("qitanid=\"");
			 int    qitanend=qitancode.indexOf("\"",qitanst+10);
			 qitanidd=qitancode.substring(qitanst+9,qitanend);
		 }
		 
			String sumplaycount = "http://cache.video.qiyi.com/jp/pc/" + id
					+ "/?callback=window.Q.__callbacks__.cbgt6rz7";
			String summcode = null;
			try {
				summcode = dongmanFunction.visitURL(sumplaycount);
			} catch (IOException e4) {
				// TODO Auto-generated catch block
				e4.printStackTrace();
			}
			dongmanFunction.Save(allcontent, sumplaycount, "Sumplaycount", url,
					summcode);
			
		 String commurl="http://api.t.iqiyi.com/qx_api/comment/get_video_comments?aid="+qitanidd+"&categoryid=10&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1"+"&page_size=10&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="+qitanidd+"&sort=hot&t=0.2906751498985618&tvid="+tv;
			String commcontent=null;
			try {
				commcontent = dongmanFunction.visitURL(commurl);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, commurl, "Comment", url,  commcontent);
		 
			String yurl="http://api.t.iqiyi.com/qx_api/comment/review/get_review_list?aid="+qitanidd+"&categoryid=10&cb=fnsucc&escape=true&need_reply=true&need_total=1&page=1&page_size=5&page_size_reply=50&qitan_comment_type=1&qitancallback=fnsucc&qitanid="+qitanidd+"&sort=hot&t=0.9215203301954558&tvid="+tv;
			   String ycontent=null;
				try {
					ycontent = dongmanFunction.visitURL(yurl);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				dongmanFunction.Save(allcontent, yurl, "Ycomment", url,  ycontent);
			
		
		 String precentge="http://cache.video.qiyi.com/pc/pr/"+id+"/playCountPCMobileCb?callback=playCountPCMobileCb";
		 String preContent = null;
		try {
			preContent = dongmanFunction.visitURL(precentge);
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
			try {
				Thread.sleep(i);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dongmanFunction.Save(allcontent, precentge, "precentge", url, preContent);
		

			 String reference="http://mixer.video.iqiyi.com/jp/recommend/videos?referenceId="+tv+"&albumId="+id+"&channelId=1&cookieId=c15080ccea46c39cb733eec29be0e154&withRefer=true&area=zebra&size=10&type=video&pru=&callback=window.Q.__callbacks__.cbhdjhdv";
			 String refContent=null;
			try {
				refContent = dongmanFunction.visitURL(reference);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
				try {
					Thread.sleep(i);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				dongmanFunction.Save(allcontent, reference, "Reference", url,  refContent);  
				String upurl = "http://up.video.iqiyi.com/ugc-updown/quud.do?dataid="
						+ tv + "&type=2";

				String upcode = null;
				try {
					upcode = dongmanFunction.visitURL(upurl);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				dongmanFunction.Save(allcontent, upurl, "updown", url, upcode);
			
			String IqiyiRow ="Iqiyi PianHua"+id+"#"+time;
			if(allcontent!=null && !allcontent.equals("")) {
				String line1=IqiyiRow+" "+url+" "+date.replaceAll(" ","_")+"@@"+crawltime;
				String line2=allcontent.toString();
				CrawlerThread.saveData(line1, line2);
			}
			if(movieincon==null) {
				jdbc.log("李辉", url+"+iy", 1, "iy", url, "URL访问失败", 1);
			}
		 }
			  allcontent=null;
			  return flag;
	}

}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	
	
	
	
