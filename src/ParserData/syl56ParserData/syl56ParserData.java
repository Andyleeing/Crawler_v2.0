package ParserData.syl56ParserData;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import hbase.HBaseCRUD;
import Utils.JDBCConnection;
import Utils.TextValue;

public class syl56ParserData 
{
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	
	public String id="";
	public String title = "无";//注意：赋值为“”，才是实例化对象，开辟了堆内存空间，且内容为“”（空）；赋值为null，没实例化对象，没开辟堆内存空间，即引用（指向对象的指针）为空；
	public String otherTitle="无";//别名可能没有
	public String pictureUrl="";
	public String publicTime="";
	public String type="";
	public String language="";
	public String area="";
	public String category="";
	public String actors = "无";
	public String directors = "无";
	public String introduction="";
	public String introductionMore="";
	public String url="";
	public String crawlTime="无";
	public String prevues="";
	public String time1="";
	public String time2="无";
	public String totalPlays="";
	public int totalCount;
	public String urlTimePlayNums="无";//对应播放页的分集数据; name+time+playNum+url;
	public String guessLikes="无";
	public String infoId="";
	public String labels="";
	public String showType="无";
	public String updown="无";
	public String times="无";
	public String comments="无";
	public String quotes="无";
	public String quoteDetails="无";
	public String name="";
	
	public String key1="";
	public String key2="";
	public String key3="";
	public String key4="";
	
	public syl56ParserData(HBaseCRUD hbase, JDBCConnection jdbconn) 
	{
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}
	
	public void resolveData(String line1,String line2) 
	{
		//解析
		String website="";
		String duration="无";
		String kind="";
		String[] str = null;
		Document doc = null;
		Elements eles = null;
		Element ele = null;
		Elements eleActors = null;
		Elements eleDirectors = null;
		Elements eleHosts=null;
		String hosts = "";
		String platform="";
		int num;
		String[] contentInfo=null;
		String totalItems="";
		String[] totalTime=null;
		totalCount=0;
		String[] part=null;
		String playTitle="";
		String playTime="";
		String playNum="";
		String playUrl="";
		String guessLike="";//猜你喜欢
		String prevue="";//预告片
		String update="";//最近更新
		String updates="";
		String[] content=null;
		String statics="";
		String dyn1="无";
		String dyn2="无";
		String dyn3="无";
		String dyn4="无";
		String ups="无";
		String downs="无";
		String tagTitle="";
	    str=StringUtils.split(line1,"@");
	    if(str.length<8) return ;
		website=str[0];
		id=str[1];
		if(id.length()>20) {
		System.out.println("*********************************************"+id);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		url=str[2];
		category=str[3];
	    kind=str[4];
	   showType=str[5];
	   time1=str[6];
	   time2=str[7];

	   //抓取时间
	   SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
	   crawlTime=sdf.format(new Date(Long.parseLong(time1)));
	   
	   if(kind.equals("Info")) 
	   {
	       contentInfo=line2.split("\\*@@@\\*");
	       doc=Jsoup.parse(contentInfo[0]);
			 if(!contentInfo[0].contains("欢迎登录北邮校园网络")&&!contentInfo[0].contains("上网注销窗"))
			 {
				 ele=doc.select("[class=stage_details]").first();
				 title=ele.select("[class=stage_title]").text();
				 pictureUrl=doc.select("[class=stage_photo_pic]").select("img").attr("src");
				 eles=ele.select("[class=stage_info_item]");
				 for(Element eleOne:eles)
				 {
				      String flag=eleOne.select("[class=stage_label]").text();
				      if(flag.equals("别名："))
				      {
				          otherTitle=eles.first().text();
				          otherTitle=otherTitle.replace("别名： ", "");		     
				      }
				      else if(flag.equals("演员："))
				      {
				          eleActors=eleOne.select("a");
				          actors=eleActors.text();
				      }
				      else if(flag.equals("导演："))
				      {
				          eleDirectors=eleOne.select("a");
				          directors=eleDirectors.text();		        
				      }
				      else if(flag.equals("主持："))
				      {
				          eleHosts=eleOne.select("a");
				          hosts=eleHosts.text();
				          if(category.equals("Show"))  actors=hosts;    
				     }
				     else if(flag.equals("平台："))
				     {
				    	 platform=eleOne.text();
		      			 platform=platform.replaceAll("平台： ","");
		      			 if(category.equals("Show"))  directors=platform;
				     }
				     else if(flag.equals("地区："))
				     {
				           area=eleOne.text();
				           area=area.replaceAll("地区： ","");
				     }
				  }
				  introduction=doc.select("[class=intro_cnt]").text();
				  introductionMore=doc.select("[class=stage_info_item stage_intro]").select("input").attr("value");
				  eles=doc.select("[class=so_list so_list_s1 clearfix]").first().select("[class=so_video_name]");
				  String url2;
				  num=0;
				  for(Element ele1:eles)
				   {
				        name=ele1.text();
				        url2=ele1.select("a").attr("href");
	                    guessLike=url2+"@"+name;
				  
				        if(num==0) guessLikes=guessLike;
				        else guessLikes+="^"+guessLike;
				        num++;
				   }
				  num=0;
				  if(contentInfo[0].contains("so_list so_list_s3 clearfix"))
				   {
				    	 eles=doc.select("[class=so_list so_list_s3 clearfix]").first().select("p");
				    	 for(Element ele1:eles)
					     {
					    	   name=ele1.text();
					    	   url2=ele1.select("a").attr("href");			    	
					    	   prevue=url2+"@"+name;
					    	   if(num==0) prevues=prevue;
					    	   else prevues+="^"+prevue;
					    	   num++;
					     } 
				   }
				  else  //预告片可能不存在
				  {
				    	prevues="无"; 
				  }
				 
			 }
		
		   if(contentInfo.length==2)
		    {
			   contentInfo[1]=unicodeToChinese(contentInfo[1]);
			   contentInfo[1]=contentInfo[1].replace("&quot;", "\"");
			
			   if(!contentInfo[1].contains("欢迎登录北邮校园网络")&&!contentInfo[1].contains("上网注销窗"))
			   	{
				    publicTime=strFind(contentInfo[1], "public_time\":\"", "\"");
				    if(publicTime==null||publicTime.equals("")) publicTime="无";
				
				    type=strFind(contentInfo[1], "tname\":", ",");
				   
				    if(type!=null&&!type.equals("null"))//有的源代码本身为空
				     {
				        type=strFind(contentInfo[1], "tname\":[\"", "\"],");
					     if(type!=null&&type.contains(",")) type=type.replaceAll("\",\"", " ");
					            	 
				     }
				    if(type==null||type.equals("")) type="无";
				   
				    language=strFind(contentInfo[1], "lname\":[\"", "\"");
				    if(language==null||language.equals("")) language="无";
				  
				    totalItems=strFind(contentInfo[1], "total_items\":\"", "\"");
				    if(totalItems==null||totalItems.equals("")) totalItems="无";	
				 
				    totalPlays=strFind(contentInfo[1], "views\":\"", "\"");
				    if(totalPlays==null||totalPlays.equals("")) totalPlays="无";	
				
				    String temp=null;
				    temp=strFind(contentInfo[1], "totalCount\":", ",");
				    
				    if(temp!=null) totalCount=Integer.parseInt(temp);
			
				    totalTime=new String[totalCount];
				    temp=null;
				    temp=strFind(contentInfo[1], "data\":[", "{\"totalCount");
				     
				    if(temp!=null&&!temp.equals("null"))
				     {
				    	 part=temp.split("},");	
				         num=0;
						 for(int i=0;i<totalCount;i++)
						 {
							 playTitle=strFind(part[i], "title\":\"", "\"");
							 playTime=strFind(part[i], "totaltime\":\"", "\"");
							 playNum=strFind(part[i], "times\":\"", "\"");
							 playUrl=strFind(part[i], "url\":\"", "\"");  
							 part[i]=playTitle+"@"+playTime+"@"+playNum+"@"+playUrl;
							 if(i==0) urlTimePlayNums=part[i];
							 else urlTimePlayNums+="^"+part[i];
							 
						  }
						  if(urlTimePlayNums==null||urlTimePlayNums.equals("")) urlTimePlayNums="无";
						
					 }
			   	}
		    }
	    }
	   else //if(kind.equals("Play")) //Play 
	   {
	      if(str.length==9) infoId=str[8];
	      showType=str[5];
	      content=line2.split("\\*@@@\\*");
	      statics=content[0];
	      if(content.length>=2&&content[1]!=null&&!content[1].equals("")&&!content[1].equals("null")) 
	    	{
	    	  		dyn1=content[1];  
	    	  		
	    	}
	      if(content.length>=3&&content[2]!=null&&!content[2].equals("")&&!content[2].equals("null")) 
	      	{
	    	  		dyn2=content[2];
	    	  		dyn2=dyn2.replace("&quot;", "\"");
	      	}
	      if(content.length>=4&&content[3]!=null&&!content[3].equals("")&&!content[3].equals("null")) 
	      	{
	    	  		dyn3=content[3];
	    	  		dyn3=dyn3.replace("&quot;", "\"");
	      	}
	      if(content.length>=5&&str.length==5&&content[4]!=null&&!content[4].equals("")&&!content[4].equals("null")) 
	      	{
	    	  		dyn4=content[4];	
	    	  		dyn4=dyn4.replace("&quot;", "\"");	          
	      	}
	      doc=Jsoup.parse(statics);
	      title=doc.select("[id=video_title_text]").text();
	      labels=doc.select("[name=keywords]").attr("content");
	      labels=labels.replaceAll(",视频在线观看,视频观看,视频在线播放", "");
	      labels=labels.replaceAll(",", " ");
	      if(dyn1!=null&&!dyn1.equals("")&&!dyn1.equals("无"))
	       {
	    	  	dyn1=dyn1.replace("&quot;", "'");
	           times=strFind(dyn1,"'times':",",");	
	           if(times==null||times.equals("")) times="无";
	           if(times.contains("欢迎登录北邮校园网络")) times="无";
	           if(times.contains("上网注销窗")) times="无";
		        ups=strFind(dyn1,"'ups':",",");
		        if(ups!=null&&ups.contains("'")) ups=ups.replaceAll("'","");
		        downs=strFind(dyn1,"'downs':",",");
		        if(downs!=null&&downs.contains("'"))  downs=downs.replaceAll("'","");   
		        if(ups==null||ups.equals("")) ups="无";
		        if(downs==null||downs.equals("")) downs="无";
		        
		        updown=ups+"/"+downs;
		        if(updown==null||updown.equals("")||(ups.equals("无")&&downs.equals("无"))) updown="无";
		        if(updown.contains("欢迎登录北邮校园网络")) updown="无";
		        if(updown.contains("上网注销窗")) updown="无";
		      
	        }
	      	if(dyn2!=null&&!dyn2.equals("")&&!dyn2.equals("无"))
	        {
	           comments=strFind(dyn2,"ctTotal\":","}");
	          
	           if(comments==null||comments.equals("")||comments.equals("null")) comments="无";
	           else if(comments.contains("欢迎登录北邮校园网络")) comments="无";
		       else if(comments.contains("上网注销窗")) comments="无";
		       else if(comments.contains(",")) 
		       {
		    	   comments=strFind(comments, "", ",");		
		       }
	        }
	      	else
	      	{ 
	    	  	 comments="无";
	      	}
	      	if(dyn3!=null&&!dyn3.equals("")&&!dyn3.equals("无"))
	        {
	           quotes=strFind(dyn3, "quoteCount\":", "}"); 
	           if(quotes==null||quotes.equals("")) 
	           {
	        	  quotes="无";
	           }
	           if(quotes.contains("欢迎登录北邮校园网络")) quotes="无";
	           if(quotes.contains("上网注销窗")) quotes="无";
		        String temp=null;
		        String domain=null;
		        String count2=null;
		        temp=strFind(dyn3, "quotesite\":[", "],\"quoteCount");
		        if(temp==null||temp.equals("")||temp.equals("null")) 
		        {
		        	    quoteDetails="无";
		        }		        
		        else if(temp!=null)
		          {
		            part=temp.split("},");
		            num=0;
		            for(int i=0;i<part.length;i++)
			            {
		            	part[i]=part[i]+"}";
			          
			            domain=strFind(part[i], "domain\":\"rp_", "\"");
			            count2=strFind(part[i], "count\":", "}");
			            part[i]=domain+"@"+count2;
			            if(num==0) quoteDetails=part[i];
			            else quoteDetails+="^"+part[i];
			          
			            }
		            if(quoteDetails==null||quoteDetails.equals("")) quoteDetails="无";
		        
		         }
		    }
	      if(dyn4!=null&&!dyn4.equals("")&&!dyn4.equals("无"))
	        {
	       
	         String temp=null;
	         temp=strFind(dyn4, "data\":[" ,"]");
	        
	         String liketimes;
	         String subject;
	         String likeUrl;
	         if(temp!=null&&!temp.equals("null"))
	            {
	            part=temp.split("},");
	            num=0;
	            for(int i=0;i<part.length;i++)
			        {
	            	part[i]=part[i]+"}";
	            
	            	subject=strFind(part[i], "Subject\":\"", "\",\"id");
	            	likeUrl=strFind(part[i], "url\":\"", "\",");
	            
	            	part[i]=subject+"@"+likeUrl;
	            	if(num==0) guessLikes=part[i];
	            	else guessLikes+="^"+part[i];
	            	num++;
			        }
	            if(guessLikes==null||guessLikes.equals("")||guessLikes.equals("null@null")) guessLikes="无";
	            }
	        }	
	   }
	   save(); //保存数据到hbase,mysql;
	}
	
	public void save()
	{
//		movieinfoToHbase();
//		movieinfoToMySQL();
//		
//		moviedynamicToHbase();
//		moviedynamicToMySQL();
//		
//		videoinfoToHbase();
//		videoinfoToMySQL();
//		
//		videodynamicToHbase();
//		videodynamicToMySQL();
		key1="56"+"+"+id;
		key2="56"+"+"+id+"+"+time1;	
		if (infoIsExist(key1, "movie") == 0) {
			movieinfoToHbase();
			movieinfoToMySQL();
		}
		
		if (moviedynamicIsExist(key2) == 0) {
			moviedynamicToHbase();
			moviedynamicToMySQL();
		}
		
		if (infoIsExist(id, "video") == 0) {
			videoinfoToHbase();
			videoinfoToMySQL();
		}	
		videodynamicToHbase();
		videodynamicToMySQL();
		
	}
	
	public void movieinfoToHbase()
	{  
		//table1
	    String[] rows = null;
 		String[] colfams = null;
 		String[] quals = null;
 		String[] values = null;
 		key1="56"+"+"+id;		
		rows = new String[]   { key1,        key1,     key1,  key1,        key1,       key1,      key1,    key1,  key1,  key1,      key1,       key1,      key1,        key1,              key1,   key1,       key1};	
 		colfams = new String[]{"R",         "R",      "B",   "B",         "B",        "B",       "B",     "B",   "B",   "B",       "B",        "B",       "B",         "B",               "B",    "B",        "C"};
		quals = new String[]  {"inforowkey","website","name","pictureURL","othername","time",    "lan",   "area","type","director","mainactor","category","summarize", "introductionMore","url",	"crawltime","prevueurls"};
		values = new String[] { id,         "56",      title, pictureUrl,  otherTitle, publicTime,language,area,  type,  directors, actors,     category,  introduction,introductionMore,  url,    crawlTime,  prevues};
		
		try 
		{
			hbase.putRows("movieinfo", rows, colfams, quals, values);
			hbase.putRows("movieinfobak", rows, colfams, quals, values);//备份
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}	
		
		rows = null;
		colfams = null;
		quals =null;
		values = null;   		
	}
	
	public void movieinfoToMySQL()
	{	
		if(publicTime.equals("")||publicTime.equals("无")) return ;
		String timeString = "";
		int timeint = 0;
	    timeint = Integer.parseInt(publicTime.replace("-", "").replace(" ", ""));
		
		String lanString = "";
		if(language.contains("@"))
		{
			int index=language.indexOf("@");
			if(index >= 0) lanString = language.substring(0,index);
		}
		else lanString = language;
		
		String areaString = "";
		if(area.contains("@"))
		{
			int index = area.indexOf("@");
			if(index >= 0) areaString = area.substring(0,index);
		}
		else areaString = area;
		
		String directorString = "";
		String[] directorSplits = null;
		directorSplits= directors.split(" ");//导演是用空格分开的
		
		String mainactorString = "";
		String[] mainactorSplits = null;
		mainactorSplits = actors.split(" ");//主演是用空格分开的		
		
		if(!key1.contains("+")) return; //如果keyString不包含+,则排除，筛选出形如56+1617的数据
			
		if (type == "") return;
		String[] typeSplits = type.split(" ");
		
		//table1:movieinfo
		ArrayList<TextValue> values = new ArrayList<TextValue>();

		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = key1;
		values.add(rowkeytv);
		
		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		nametv.value = title;
		values.add(nametv);
		
		for(int i = 0;i < typeSplits.length;i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i+1);
			typetv.value = typeSplits[i];
			values.add(typetv);
			if(i == 2)
				break;
		}
		
		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "56";
		values.add(namewebsite);
		
		String categoryString="";
		if(category.equals("Movie")) categoryString="movie";
		else if(category.equals("Tv")) categoryString="tv";
		else if(category.equals("Show")) categoryString="zongyi";
		else if(category.equals("Cartoon")) categoryString="dongman";
		
		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		categorytv.value = categoryString;
		values.add(categorytv);
		
		TextValue lantv = new TextValue();
		lantv.text = "lan";
		lantv.value = lanString;
		values.add(lantv);
		
		TextValue areatv = new TextValue();
		areatv.text = "area";
		areatv.value = areaString;
		values.add(areatv);
		
		if(mainactorSplits != null) {
			for (int i = 0; i < mainactorSplits.length; i++) {
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				actortv.value = mainactorSplits[i];
				values.add(actortv);
				if (i == 2)
					break;
			}
		}
		
		if(directorSplits != null) {
			for (int i = 0; i < directorSplits.length; i++) {
				TextValue directortv = new TextValue();
				directortv.text = "director" + (i + 1);
				directortv.value = directorSplits[i];
				values.add(directortv);
				if (i == 2)
					break;
			}
		}
		
		TextValue durationtv = new TextValue();
		durationtv.text = "duration";
		durationtv.value = -1;
		values.add(durationtv);
		
		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = -1;
		values.add(pricetv);
		
	
		TextValue summarizetv = new TextValue();
		summarizetv.text = "summarize";
		summarizetv.value = introduction;
		values.add(summarizetv);
		
		TextValue yeartv = new TextValue();
		yeartv.text = "year";
		yeartv.value = Integer.parseInt(publicTime.replace("-", "").replace(" ", "").substring(0,4));
		values.add(yeartv);
		
		TextValue timetv = new TextValue();
		timetv.text = "time";
		timetv.value = timeint;
		values.add(timetv);
		
		TextValue ytimetv = new TextValue();
		ytimetv.text = "ytime";
		ytimetv.value = -1;
		values.add(ytimetv);
		
		TextValue crawltimetv=new TextValue();//加入抓取时间，这里=导入时间（每天导一次）
		crawltimetv.text="crawltime";
		crawltimetv.value=crawlTime;
		values.add(crawltimetv);		
		
		jdbconn.insert(values, "movieinfo");
	
	}
	
	public void moviedynamicToHbase()
	{
		//table2
	
	 	key2="56"+"+"+id+"+"+time1;	
	 	String[] rows = new String[]   { key2,        key2,     key2,      key2,         key2,  key2,     	 	 key2,      key2,            key2,       key2};	
	 	String[] colfams = new String[]{"R",         "R",      "R",        "R",         "C",   "C",     		"C",       "C",             "C",        "C"};
	 	String[] quals = new String[]  {"inforowkey","website","timestamp","timestamp2","name","sumplaycount","jishu",   "playtimenums",  "guess",    "crawltime"};
	 	String[] values = new String[] { id,         "56",      time1,      time2,       title, totalPlays, ""+totalCount,urlTimePlayNums, guessLikes, crawlTime};//空串+整数，将整数转化为字符串
			
		try 
		{
			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak", rows, colfams, quals, values);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}	
	
		rows = null;
		colfams = null;
		quals =null;
		values = null;   
	}
	
	public void moviedynamicToMySQL()
	{
		
		ArrayList<TextValue> values = new ArrayList<TextValue>();
		
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
	
		rowkeytv.value = key2;
	
		values.add(rowkeytv);

		long sumplay = ConvertToLong(totalPlays);
		
		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = sumplay;
		values.add(sumPlayCounttv);

		TextValue uptv = new TextValue();
		uptv.text = "up";
		uptv.value = -1;
		values.add(uptv);
		
		TextValue downtv = new TextValue();
		downtv.text = "down";
		downtv.value = -1;
		values.add(downtv);
		
		TextValue scoretv = new TextValue();
		scoretv.text = "score";
		scoretv.value = -1;
		values.add(scoretv);				
		
		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// n 2:y
		values.add(flagtv);

		TextValue commenttv = new TextValue();
		commenttv.text = "comment";
		commenttv.value = -1;
		values.add(commenttv);	
		
		TextValue todayPlayCounttv = new TextValue();
		todayPlayCounttv.text = "todayPlayCount";
		todayPlayCounttv.value = -1;
		values.add(todayPlayCounttv);	
		
		TextValue mantv = new TextValue();
		mantv.text = "man";
		mantv.value = -1;
		values.add(mantv);	
		
		TextValue womentv = new TextValue();
		womentv.text = "women";
		womentv.value = -1;
		values.add(womentv);	
		
		long timeStamp = ConvertToLong(time1.substring(0,10));
		
		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = timeStamp;
		values.add(timestamptv);

		TextValue movienametv = new TextValue();
		movienametv.text = "movieName";
		movienametv.value = title;
		values.add(movienametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "56";
		values.add(namewebsite);

		TextValue referencetv = new TextValue();
		referencetv.text = "reference";
		referencetv.value = 0;
		values.add(referencetv);
		
		//category统一格式
		String categoryString="";
		if(category.equals("Movie")) categoryString="movie";
		else if(category.equals("Tv")) categoryString="tv";
		else if(category.equals("Show")) categoryString="zongyi";
		else if(category.equals("Cartoon")) categoryString="dongman";
		
		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		categorytv.value = categoryString;
		values.add(categorytv);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(timeStamp
				+ "000")));
		if (sumplay > 0)
		{
			jdbconn.insert(values, "moviedynamic" + sd);
			System.out.println("表名="+"moviedynamic"+sd);
		}
	}
	
	public void videoinfoToHbase()
	{
		//table3
	    String[] rows = null;
 		String[] colfams = null;
 		String[] quals = null;
 		String[] values = null;
 		key3="56"+"+"+id+"+"+infoId;		
		rows = new String[]   { key3,        key3,     key3,       key3,   key3, key3,     key3,      key3};	
 		colfams = new String[]{"R",         "R",      "R",   		  "B",   "B",  "B",      "B",       "B"};
		quals = new String[]  {"playrowkey","website","inforowkey","name","url","labels", "showtype","crawltime"};
		values = new String[] { id,         "56",      infoId, 		title, url,  labels,   showType,  crawlTime };			
		
		try 
		{
			hbase.putRows("videoinfo", rows, colfams, quals, values);
			hbase.putRows("videoinfobak", rows, colfams, quals, values);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}	
		
		rows = null;
		colfams = null;
		quals =null;
		values = null;      
	}
	
	public void videoinfoToMySQL()
	{
		ArrayList<TextValue> values = new ArrayList<TextValue>();
		
		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = infoId;
		values.add(inforowkeytv);
		
		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = id;
		values.add(playrowkeytv);
		
		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "56";
		values.add(namewebsite);
		
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = key3;
		values.add(rowkeytv);
		
		TextValue nametv = new TextValue();
		nametv.text = "name";
		nametv.value = title;
		values.add(nametv);		
		
		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = showType;
		values.add(showtypetv);	
		
		TextValue crawltimetv=new TextValue();//加入抓取时间，这里=导入时间（每天导一次）
		crawltimetv.text="crawltime";
		crawltimetv.value=crawlTime;
		values.add(crawltimetv);
		
		jdbconn.insert(values, "videoinfo");
		
	}
	
	public void videodynamicToHbase()
	{
		//table4
		String[] rows = null;
	    String[] colfams = null;
	    String[] quals = null;
	    String[] values = null;
	 	key4="56"+"+"+id+"+"+infoId+"+"+time1;		
		rows = new String[]   { key4,        key4,     key4,        key4,       key4,        key4,    key4, 			 key4,     key4,   key4,			key4,       key4};	
	 	colfams = new String[]{"R",         "R",      "R",   		  "R",        "R",         "C",     "C",  			"C",      "C",    "C",				"C",       "C" };
		quals = new String[]  {"playrowkey","website","inforowkey","timestamp","timestamp2","updown","sumplaycount","comment","quote","quoteDetails","guess",   "crawltime"};
		values = new String[] { id,         "56",      infoId, 		time1,      time2, 	    updown,  times,         comments, quotes, quoteDetails,  guessLikes,crawlTime};
	
		try 
		{
				hbase.putRows("videodynamic", rows, colfams, quals, values);
				hbase.putRows("videodynamicbak", rows, colfams, quals, values);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}	

		rows = null;
		colfams = null;
		quals =null;
		values = null;   		
	}
	
	public void videodynamicToMySQL()
	{
		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = key4;
		values.add(rowkeytv);

		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = infoId;
		values.add(inforowkeytv);

		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = id;
		values.add(playrowkeytv);

		int sumplay=-1;
		if(!times.contains("无"))//排除非法情况
		sumplay = ConvertToInt(times);
		
		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = sumplay;
		if(sumplay==-1) return ;
		values.add(sumPlayCounttv);

		int up=-1;
		int down=-1;
		int index = updown.indexOf("/");
		if (index > 0) 
		{
			up = ConvertToInt(updown.substring(0, index));
			down = ConvertToInt(updown.substring(index + 1));
		}
		
		TextValue uptv = new TextValue();
		uptv.text = "up";
		uptv.value = up;
		values.add(uptv);

		TextValue downtv = new TextValue();
		downtv.text = "down";
		downtv.value = down;
		values.add(downtv);

		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// n 2:y
		values.add(flagtv);

		int commentCount=-1;
		if(!comments.equals("无"))//排除非法情况
		commentCount = ConvertToInt(comments);
		
		TextValue commenttv = new TextValue();
		commenttv.text = "comment";
		commenttv.value = commentCount;// n 2:y
		values.add(commenttv);

		String timeString = time1.substring(0,10);
		long timeStamp = ConvertToLong(timeString);
		
		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = timeStamp;
		values.add(timestamptv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "56";
		values.add(namewebsite);
	
		TextValue collecttv = new TextValue();
		collecttv.text = "collect";
		collecttv.value = -1;
		values.add(collecttv);
		
		TextValue outsidetv = new TextValue();
		outsidetv.text = "outside";
		outsidetv.value = -1;
		values.add(outsidetv);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(timeStamp
				+ "000")));
		
		jdbconn.insert(values, "videodynamic" + sd);

	}
	
	public static long ConvertToLong(String str) 
	{
		long value = -1;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Long.parseLong(str);
		} catch (Exception e) {
		}
		return value;
	}
	
	public static int ConvertToInt(String str) {
		int value = -1;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return value;
	}
	
	public String unicodeToChinese(String unicode)
	{
		String str=null;
		StringEscapeUtils seu=new StringEscapeUtils();
		str=seu.unescapeJava(unicode);
		str=str.replaceAll("&nbsp;", "");
		return str;
	}
	
	public String strFind(String str,String tagStart,String tagEnd)//串查找：查找两个字符串中间的子串
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
	
	//add by hanjiaxing
	public int infoIsExist(String rowkey, String tabletype) {
		int count = 0;
		count = jdbconn.executeQueryCount("select count(*) as count from " + tabletype
				+ "info where rowkey = '" + rowkey + "'");
		return count;
	}
	//add by hanjiaxing 
	public int moviedynamicIsExist(String rowkey) {
		int count = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(time1)));
		count = jdbconn.executeQueryCount("select count(*) as count from moviedynamic" + sd 
				+ " where rowkey = '" + rowkey + "' and timestamp = '" + time1.substring(0,time1.length()-3)+ "'");
		return count;
	}
	
}
