package ParserData.SohuParserData;

import hbase.HBaseCRUD;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import HDFS.HDFSCrudImpl;
import Utils.JDBCConnection;
import Utils.TextValue;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;
import jxHan.Crawler.WebSite.Base.GlobalData;

public class SohuParser {
	private static SohuParser parser = null;
	public static String timestamp;
	public long time;
	public static FileWriter fw = null;
	public ArrayList<SohuParser> pool;
	public static ArrayList<String> urls = new ArrayList<String>();
	public static HashSet<String> episodeurls = new HashSet<String>();
	public static String filelock = "filelock";
	public HDFSCrudImpl hdfs = new HDFSCrudImpl();
	public  HBaseCRUD hbase ;
	public JDBCConnection jdbconn;
	public static int fileCount = 0;
	public static ArrayList<String> album = new ArrayList<String>();

	
	public SohuParser(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}
	public String[] zongyiBaseInfo(String source, String category) {
		String temp = source;
		String host = null;
		String Hhost = null;
		String temphost = null;
		int indexStart = 0, indexEnd = 0;
		int index = 0;
		String Halbum = null;
		indexStart = temp.indexOf("<title>");
		if (indexStart < 0)
			return null;
		temp = temp.substring(indexStart + 7);
		indexEnd = temp.indexOf("</title>");
		if (indexEnd < 0)
			return null;
		Halbum = temp.substring(0, indexEnd);
		indexStart = temp.indexOf("主持人");
		if (indexStart < 0)
			return null;
		temp = temp.substring(indexStart);
		indexEnd = temp.indexOf("</li>");
		if (indexEnd < 0)
			return null;
		temphost = temp.substring(0, indexEnd);
		while (true) {
			indexStart = temphost.indexOf("Variety_info_others");
			if (indexStart > 0) {
				temphost = temphost.substring(indexStart + 21);
				temp = temp.substring(indexStart + 21);
			} else
				break;
			indexEnd = temphost.indexOf("</a>");
			if (indexEnd > 0)
				host = temphost.substring(0, indexEnd);
			else
				break;
			if (Hhost != null)
				Hhost = Hhost + host;
			else
				Hhost = host;
			indexStart = temphost.indexOf("Variety_starpage_vv");
			if (indexStart < 0)
				break;
			else
				Hhost = Hhost + "@";
		}
		String Hplaytv = null;
		if (temp.indexOf("播出") < 0)
			return null;
		indexStart = temp.indexOf("Variety_info_others");
		if (indexStart > 0) {
			temp = temp.substring(indexStart + 21);
			indexEnd = temp.indexOf("</a>");
			if (indexEnd > 0)
				Hplaytv = temp.substring(0, indexEnd);
		} else
			Hplaytv = "";
		String Harea = null;
		if (temp.indexOf("地区") < 0)
			return null;
		indexStart = temp.indexOf("Variety_info_others");
		if (indexStart > 0) {
			temp = temp.substring(indexStart + 21);
			indexEnd = temp.indexOf("</a>");
			if (indexEnd > 0)
				Harea = temp.substring(0, indexEnd);
		} else
			Harea = "";
		String type = null;
		String Htype = null;
		if (temp.indexOf("类型") < 0)
			return null;
		while (true) {
			indexStart = temp.indexOf("Variety_info_others");
			if (indexStart > 0)
				temp = temp.substring(indexStart + 21);
			else
				break;
			indexEnd = temp.indexOf("</a>");
			if (indexEnd > 0)
				type = temp.substring(0, indexEnd);
			else
				break;
			if (Htype != null)
				Htype = Htype + type;
			else
				Htype = type;
			indexStart = temp.indexOf("Variety_search_vv");
			if (indexStart < 0)
				break;
			else
				Htype = Htype + "@";
		}
		String Hintro = null;
		index = source.indexOf("full_intro");
		if (index < 0)
			Hintro = "";
		temp = source.substring(index);
		index = temp.indexOf("\">");
		if (index < 0)
			Hintro = "";
		temp = temp.substring(index + 2);
		index = temp.indexOf("</span>");
		if (index < 0)
			Hintro = "";
		Hintro = temp.substring(0, index);
		String[] nums = { Halbum, Hhost, Hplaytv, Harea, Htype, Hintro };
		if (nums != null) {
			for (int j = 0; j < nums.length; j++) {
				if (nums[j] == null)
					nums[j] = "";
			}
		}
		return nums;
	}

	public String[] ysdmBaseInfo1(String source, String category) {// 只锟斤拷锟斤拷锟接帮拷锟�
		String temp = source;
		String Halbum = null;
		String Hdirector = null;
		int indexStart = 0;
		int indexEnd = 0;
		int index = 0;
		index = temp.indexOf("blockRA bord clear");
		if (index < 0)
			return null;
		temp = temp.substring(index);
		indexStart = temp.indexOf("<span>");
		indexEnd = temp.indexOf("</span>");
		if (indexStart < 0 || indexEnd < 0 || indexStart > indexEnd)
			return null;
		Halbum = temp.substring(indexStart + 6, indexEnd);
		indexStart = temp.indexOf("<p>");
		indexEnd = temp.indexOf("评分");
		if (indexEnd < 0 || indexStart < 0)
			return null;
		temp = temp.substring(indexStart, indexEnd);
		// 锟斤拷锟捷匡拷始
		indexEnd = temp.indexOf("</p>");
		String tempDirec = null;
		String director = null;
		tempDirec = temp.substring(0, indexEnd);
		temp = temp.substring(indexEnd + 4);
		index = tempDirec.indexOf("\">");
		while (index > -1) {
			tempDirec = tempDirec.substring(index + 2);
			if (tempDirec.indexOf("</a>") < 0)
				break;
			director = tempDirec.substring(0, tempDirec.indexOf("</a>"));
			index = tempDirec.indexOf("\">");
			if (Hdirector != null)
				Hdirector = Hdirector + director;
			else
				Hdirector = director;
			if (index < 0)
				break;
			else
				Hdirector = Hdirector + "@";
		}
		// System.out.println(Hdirector);
		// 锟斤拷锟捷斤拷锟斤拷
		String Hactor = null;
		// if (category.indexOf("yingshi")>=0) {
		indexEnd = temp.indexOf("</p>");
		String tempActor = null;
		String actor = null;
		tempActor = temp.substring(0, indexEnd);
		temp = temp.substring(indexEnd + 4);
		index = tempActor.indexOf("\">");
		while (index > -1) {
			tempActor = tempActor.substring(index + 2);
			if (tempActor.indexOf("</a>") < 0)
				break;
			actor = tempActor.substring(0, tempActor.indexOf("</a>"));
			index = tempActor.indexOf("\">");
			if (Hactor != null)
				Hactor = Hactor + actor;
			else
				Hactor = actor;
			if (index < 0)
				break;
			else
				Hactor = Hactor + "@";
		}
		// System.out.println(Hactor);
		// }
		indexStart = temp.indexOf("class=\"d1\"");
		if (indexStart < 0)
			return null;
		temp = temp.substring(indexStart + 11);
		indexEnd = temp.indexOf("<a id=\"fullinfo\"");
		String Hintro = null;
		Hintro = temp.substring(0, indexEnd);
		// 锟斤拷锟斤拷锟斤拷
		index = temp.indexOf("<p>");
		temp = temp.substring(index);
		// 锟斤拷菘锟绞�
		indexEnd = temp.indexOf("</p>");
		String Hyear = null;
		String tempYear = null;
		tempYear = temp.substring(0, indexEnd);
		temp = temp.substring(indexEnd + 4);
		index = tempYear.indexOf("\">");
		if (index < 0)
			return null;
		tempYear = tempYear.substring(index + 2);
		Hyear = tempYear.substring(0, tempYear.indexOf("年"));
		// 锟斤拷萁锟斤拷锟�
		// 锟斤拷锟酵匡拷始
		indexEnd = temp.indexOf("</p>");
		String tempType = null;
		String type = null;
		String Htype = null;
		tempType = temp.substring(0, indexEnd);
		temp = temp.substring(indexEnd + 4);
		index = tempType.indexOf("\">");
		while (index > -1) {
			tempType = tempType.substring(index + 2);
			type = tempType.substring(0, tempType.indexOf("</a>"));
			index = tempType.indexOf("\">");
			if (Htype != null)
				Htype = Htype + type;
			else
				Htype = type;
			if (index < 0)
				break;
			else
				Htype = Htype + "@";
		}
		// "name", "director", "mainactor", "summarize",
		// "year","categories","area"
		String Harea = "";
		// System.out.println("year-->"+Hyear);
		String[] nums = { Halbum, Hdirector, Hactor, Hintro, Hyear, Htype,
				Harea };
		for (int j = 0; j < nums.length; j++) {
			if (nums[j] == null)
				nums[j] = "";
		}
		return nums;
	}

	public String parsePart(String part) {
		int index = 0;
		index = part.indexOf("上映时间");
		if (index > 0) {// 涓婃槧鏃堕棿
			part = part.substring(index);
			index = part.indexOf("</span>");
			if (index < 0)
				return null;
			part = part.substring(index + 7);
			return part;
		}
		String tempDirec = part;
		String director = null;
		String Hdirector = null;
		index = tempDirec.indexOf("</span>");
		if (index > 0)
			tempDirec = tempDirec.substring(index + 7);
		index = tempDirec.indexOf("\">");
		if (index < 0)
			return null;
		// 閮藉競鍓/</span><a
		// href="http://so.tv.sohu.com/list_p1101_p2101112_p3_p4_p5_p6_p7_p8_p9.html"
		// title="" target="_blank">鎮枒鍓鎮枒鍓�
		while (true) {
			tempDirec = tempDirec.substring(index + 2);
			if (tempDirec.indexOf("</a>") < 0)
				break;
			director = tempDirec.substring(0, tempDirec.indexOf("</a>"));
			index = tempDirec.indexOf("</span>");
			if (index > 0)
				tempDirec = tempDirec.substring(index);
			index = tempDirec.indexOf("\">");
			if (Hdirector != null)
				Hdirector = Hdirector + director;
			else
				Hdirector = director;
			if (index < 0)
				break;
			else
				Hdirector = Hdirector + "@";
		}
		return Hdirector;
	}

	public String[] ysdmBaseInfo0(String source, String category) {
		if (source == null || source.equals(""))
			return null;
		String temp = source;
		String Halbum = null;
		Halbum = getAlbum(source);
		int indexStart = 0;
		int indexEnd = 0;
		int index = 0;
		indexStart = temp.indexOf("class=\"vname\"");
		indexEnd = temp.indexOf("评分");
		if (indexStart < 0 || indexEnd < 0 || indexStart > indexEnd)
			return null;
		temp = temp.substring(indexStart, indexEnd);
		indexEnd = temp.indexOf("</span>");
		Halbum = temp.substring(14, indexEnd);
		index = temp.indexOf("别名");
		if (index > 0) {
			index = temp.indexOf("</li>");
			temp = temp.substring(index + 5);
		}
		String[] columns = { "上映时间","地区","类型","导演","主演" };
		String[] columnValues = new String[5];
		for (int j = 0; j < columns.length; j++) {
			String part = null;
			indexStart = temp.indexOf("<li");
			indexEnd = temp.indexOf("</li>");
			if (indexStart < 0 || indexEnd < 0 || indexStart > indexEnd)
				return null;
			part = temp.substring(indexStart, indexEnd);
			index = part.indexOf(columns[j]);
			if (index < 0)
				continue;
			else
				columnValues[j] = parsePart(part);
			temp = temp.substring(indexEnd + 5);
		}
		String Hintro = null;
		index = source.indexOf("full_intro");
		if (index < 0) {
			Hintro = "";
			return null;
		}
		temp = source.substring(index);
		index = temp.indexOf("\">");
		if (index < 0) {
			Hintro = "";
			return null;
		}
		temp = temp.substring(index + 2);
		index = temp.indexOf("</span>");
		if (index < 0) {
			Hintro = "";
			return null;
		}
		Hintro = temp.substring(0, index);
		String[] nums = { Halbum, columnValues[3], columnValues[4], Hintro,
				columnValues[0], columnValues[2], columnValues[1] };
		for (int j = 0; j < nums.length; j++) {
			if (nums[j] == null)
				nums[j] = "";
		}
		return nums;
	}

	public void dyInfoParser(String[] arrs, String content,
			JDBCConnection jdbconn) {

		// arrs :sh, rowKey,info,url,date, category
		// content:source mark count1 count2
		ArrayList<String> dyn = new ArrayList<String>();
		dyn = getDynamics(content);
		if (dyn.size() < 4)
			return;
		for (int j = 0; j < dyn.size(); j++) {
			if (dyn.get(j).indexOf("502 Bad Gateway") >= 0) {
				jdbconn.log("郑玲", arrs[1], 1, "sh", arrs[1], "播放量没有抓到", 2);		
				return;
			}
		}
		int free = Utils.isPaid(dyn.get(0));
		String category = arrs[5];
		String sumplaycount = dyn.get(2).replaceAll("\\.","");//sumplay
		String yesterdaycount = dyn.get(3);
		String summari="";
		
		double markTotal = 0.0;
		double peopleTotal = 0.0;
		double[] marks = new double[2];
		marks = Utils.getMarks(dyn.get(1));
		markTotal = marks[1];
		peopleTotal = marks[0];
		String source = dyn.get(0);
		int index=source.indexOf("<span class=\"full_intro\"");
		if(index>=0) {
			int end=source.indexOf("</span>",index);
			if(end>index) {
		 String temp=source.substring(index, end).replaceAll("&nbsp;", "");
		 summari=temp.substring(temp.indexOf("none\">")+6);
			}
		}
	
		String timestamp = arrs[4].substring(0,10);
		String albumName = getAlbum(source);
		String[] movieinfos = new String[7];
		movieinfos = parseMovieBaseinfo(source);
		String inforowkey = arrs[1];
		String videoinfokey = inforowkey + "+sh";
		String[] videoinforows = { videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey,videoinfokey,videoinfokey };
		String[] videoinfocolfams = { "R", "R", "B", "B", "B", "R", "B", "B",
				"B", "B", "B", "B","C","B" };
		String[] videoinfoquals = { "inforowkey", "website", "url", "category",
				"name", "timestamp", "publishYear", "area", "categories",
				"director", "mainactor","duration","score","summarize" };
		String[] videoinfovalues = { inforowkey, "sh", arrs[3], arrs[5],
				albumName, timestamp, movieinfos[0], movieinfos[1],
				movieinfos[2], movieinfos[3], movieinfos[4], movieinfos[5],movieinfos[6],summari};
		for (int j = 0; j < videoinfovalues.length; j++) {
			videoinfovalues[j] = setNull(videoinfovalues[j]);
		}
		try {
			hbase.putRows("movieinfosh", videoinforows, videoinfocolfams,
					videoinfoquals, videoinfovalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// mysql
		ArrayList<TextValue> valuesinfo = new ArrayList<TextValue>();

		// website category rowkey
		// "name", "timestamp", "publishYear", "area",
		// "categories", "director", "mainactor", "summarize"
		TextValue rowkey = new TextValue();
		TextValue categoryString = new TextValue();
		TextValue moviename = new TextValue();
		TextValue website = new TextValue();
		
		website.text = "website";
		website.value = "sh";
		valuesinfo.add(website);
		categoryString.text = "category";
		categoryString.value = arrs[5];
		valuesinfo.add(categoryString);
		rowkey.text = "rowkey";
		rowkey.value = inforowkey;
		valuesinfo.add(rowkey);
		
		moviename.text = "moviename";
		moviename.value = albumName;
		valuesinfo.add(moviename);
		TextValue timestampString = new TextValue();
		timestampString.text = "crawltime";
		timestampString.value = Utils.getDay(timestamp);
		valuesinfo.add(timestampString);
		TextValue publishYear = new TextValue();
		publishYear.text = "year";
		publishYear.value = movieinfos[0];
		valuesinfo.add(publishYear);
		TextValue area = new TextValue();
		area.text = "area";
		area.value = movieinfos[1];
		valuesinfo.add(area);
		TextValue dura = new TextValue();
		dura.text = "duration";
		dura.value = movieinfos[5];
		valuesinfo.add(dura);
		
		if(movieinfos[2]!=null) {
		String[] typeSplits = movieinfos[2].split("@");
		for (int i = 0; i < typeSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i + 1);
			typetv.value = typeSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		}
		
		if(movieinfos[3]!=null) {
		
		String[] directorSplits = movieinfos[3].split("@");
		for (int i = 0; i < directorSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "director" + (i + 1);
			typetv.value = directorSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		}
		
		if(movieinfos[4]!=null) {
		String[] actorSplits = movieinfos[4].split("@");
		for (int i = 0; i < actorSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "mainactor" + (i + 1);
			typetv.value = actorSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		}
		
		TextValue summarize = new TextValue();
		summarize.text = "summarize";
		summarize.value = summari;
		valuesinfo.add(summarize);
		if (infoIsExist(inforowkey, "movie") == 0) {
		jdbconn.insert(valuesinfo, "movieinfo");
		}

		String videodynamickey = inforowkey + "+sh+" + timestamp;
		String[] videodynamicrows = { videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey };
		String[] videodynamiccolfams = { "R", "B", "R", "B", "R", "C", "C",
				"C", "C", "C" };
		String[] videodynamicquals = { "website", "name", "inforowkey",
				"category", "timestamp", "markTotal", "peopleTotal",
				"sumplaycount", "yesterdayCount", "free" };
		String[] videodynamicvalues = { "sh", albumName, arrs[1], category,
				timestamp, markTotal + "", peopleTotal + "", sumplaycount,
				yesterdaycount, free + "" };
		for (int j = 0; j < videodynamicvalues.length; j++) {
			videodynamicvalues[j] = setNull(videodynamicvalues[j]);
		}
		try {
			hbase.putRows("moviedynamicsh", videodynamicrows,
					videodynamiccolfams, videodynamicquals, videodynamicvalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		ArrayList<TextValue> valuesdynamic = new ArrayList<TextValue>();
		// rowkey website category movieName timestamp sumPlayCount peopelNum
		// score free
		valuesdynamic.add(rowkey);
		valuesdynamic.add(website);
		valuesdynamic.add(categoryString);
		TextValue movieName = new TextValue();
		movieName.text = "movieName";
		movieName.value = albumName;
		valuesdynamic.add(movieName);
		TextValue timestampdynamic = new TextValue();
		timestampdynamic.text = "timestamp";
		timestampdynamic.value = timestamp;
		valuesdynamic.add(timestampdynamic);
		TextValue sumPlayCount = new TextValue();
		sumPlayCount.text = "sumPlayCount";
		sumPlayCount.value = sumplaycount;
		valuesdynamic.add(sumPlayCount);
		TextValue peopelNum = new TextValue();
		peopelNum.text = "peopleNum";
		peopelNum.value = peopleTotal;
		valuesdynamic.add(peopelNum);
		TextValue score = new TextValue();
		score.text = "score";
		score.value = movieinfos[6];
		valuesdynamic.add(score);
		TextValue freeString = new TextValue();
		freeString.text = "free";
		freeString.value = free;
		valuesdynamic.add(freeString);
		jdbconn.insert(valuesdynamic, "moviedynamic" + Utils.getDay(timestamp));

	}

	public ArrayList<String> getDynamics(String content) {
		int index = 0;
		ArrayList<String> dyn = new ArrayList<String>();
		while (true) {
			index = content.indexOf("*@@@*");
			String d = null;
			if (index > 0) {
				d = content.substring(0, index);
				dyn.add(d);
				// System.out.println(d);
			} else {
				dyn.add(content);
				break;
			}
			content = content.substring(index + 5);
		}
		return dyn;
	}

	public void ysdmPlayParser(String[] arrs, String content,
			JDBCConnection jdbconn) {

		ArrayList<String> dyn = new ArrayList<String>();
		dyn = getDynamics(content);// 视频动态数据
		if (dyn.size() < 5)
			return;
		for (int j = 0; j < dyn.size(); j++) {
			if (dyn.get(j).indexOf("502 Bad Gateway") >= 0) {
				jdbconn.log("李辉", arrs[6], 1, "sh", arrs[1], "播放量没有抓到", 2);	
				return;
			}
		}
		String comm = Utils.getComm(dyn.get(3));
		String[] dcGroup = new String[3];
		String playcount = "", simi = "", wstb = "", hotzy = "";
		dcGroup = getDC(dyn.get(1));
		if (dcGroup == null)
			return;
		String[] play = Utils.splitNumber(dyn.get(2), "vids");
		if (play == null)
			return;
		String epiTotalCount = play[0];
		String epiTodayCount = play[1];
		simi = getOtherSimi(dyn.get(4));//猜你
		String albumName = null;
		albumName = getAlbum(content);
		String timestamp = null;
		timestamp = arrs[4].substring(0,10);
		String videoinfokey = arrs[6] + "+" + arrs[1] + "+sh";
		String[] videoinforows = { videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey };
		String[] videoinfocolfams = { "R", "B", "R", "R", "B", "C", "R" };
		String[] videoinfoquals = { "website", "url", "playrowkey",
				"inforowkey", "name", "comment", "timestamp" };
		String[] videoinfovalues = { "sh", arrs[3], arrs[1], arrs[6],
				albumName, comm, timestamp };
		for (int j = 0; j < videoinfovalues.length; j++) {
			videoinfovalues[j] = setNull(videoinfovalues[j]);
		}
		try {
		
			hbase.putRows("videoinfosh", videoinforows, videoinfocolfams,
					videoinfoquals, videoinfovalues);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		// mysql
		// website rowkey infokey playrowkey name crawltime showtype comment

		TextValue website = new TextValue();
		TextValue inforowkey = new TextValue();
		TextValue playrowkeytv = new TextValue();
		TextValue moviename = new TextValue();
		TextValue rowkey = new TextValue();
		TextValue commenttv = new TextValue();
	
		ArrayList<TextValue> valuesinfo = new ArrayList<TextValue>();
		
		website.text = "website";
		website.value = "sh";
		valuesinfo.add(website);
	
		rowkey.text = "rowkey";
		rowkey.value = arrs[6];
		valuesinfo.add(rowkey);
		
		inforowkey.text = "inforowkey";
		inforowkey.value = arrs[6];
		valuesinfo.add(inforowkey);
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = arrs[1];
		valuesinfo.add(playrowkeytv);

		moviename.text = "name";
		moviename.value = albumName;
		valuesinfo.add(moviename);
		TextValue timestampString = new TextValue();
		timestampString.text = "crawltime";
		timestampString.value = Utils.getDay(timestamp);
		valuesinfo.add(timestampString);
	
		int commentInt = Utils.ConvertToInt(comm);
		commenttv.text = "comment";
		commenttv.value = commentInt;
		valuesinfo.add(commenttv);
		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = "正片";
		valuesinfo.add(showtypetv);
		if (infoIsExist(arrs[6], "video") == 0) {
		jdbconn.insert(valuesinfo, "videoinfo");
		}
		
		
		String videodynamickey = arrs[6] + "+" + arrs[1] + "+sh+" + timestamp;
		String[] videodynamicrows = { videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey };
		String[] videodynamiccolfams = { "R", "R", "R", "R", "C", "C", "C",
				"C", "C", "C", "C", "C" };
		String[] videodynamicquals = { "website", "playrowkey", "inforowkey",
				"timestamp", "ding", "cai", "sumplaycount", "epiTodayCount",
				"simi", "wstb", "hotzy", "comment" };
		// sumplaycount=epiTotalCount

		String[] videodynamicvalues = { "sh", arrs[1], arrs[6], timestamp,
				dcGroup[0], dcGroup[1], epiTotalCount, epiTodayCount, simi,
				wstb, hotzy, comm };
		for (int j = 0; j < videodynamicvalues.length; j++) {
			videodynamicvalues[j] = setNull(videodynamicvalues[j]);
		}
		try {
			hbase.putRows("videodynamicsh", videodynamicrows,
					videodynamiccolfams, videodynamicquals, videodynamicvalues);

		} catch (Exception e) {
			e.printStackTrace();
		}

		ArrayList<TextValue> valuesdynamic = new ArrayList<TextValue>();
		valuesdynamic.add(website);
		valuesdynamic.add(inforowkey);
		valuesdynamic.add(playrowkeytv);
		valuesdynamic.add(rowkey);
		valuesdynamic.add(commenttv);
		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = timestamp;
		valuesdynamic.add(timestamptv);
		int dingInt = Utils.ConvertToInt(dcGroup[0]);
		TextValue dingtv = new TextValue();
		dingtv.text = "up";
		dingtv.value = dingInt;
		valuesdynamic.add(dingtv);
		int caiInt = Utils.ConvertToInt(dcGroup[1]);
		TextValue caitv = new TextValue();
		caitv.text = "down";
		caitv.value = caiInt;
		valuesdynamic.add(caitv);
		TextValue sumplaycounttv = new TextValue();
		sumplaycounttv.text = "sumplaycount";
		sumplaycounttv.value = Utils.ConvertToInt(epiTotalCount);
		valuesdynamic.add(sumplaycounttv);
		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;
		valuesdynamic.add(flagtv);
		jdbconn.insert(valuesdynamic, "videodynamic" + Utils.getDay(timestamp));
	
		String[] simiParts = simi.split("@");
		if (simiParts == null)
			return;
		for (int j = 0; j < simiParts.length; j++) {
		
			String[] pairs = simiParts[j].split("\\$");
			if (pairs == null || pairs.length < 2)
				return;
			ArrayList<TextValue> references = new ArrayList<TextValue>();
			TextValue rowkeytv = new TextValue();
			rowkeytv.text = "rowkey";
			rowkeytv.value = Utils.getRowkey(pairs[1]);
			references.add(rowkeytv);
			TextValue websitet = new TextValue();
			websitet.text = "website";
			websitet.value ="sh";
			references.add(websitet);
			TextValue referencetv = new TextValue();
			referencetv.text = "reference";
			referencetv.value = 1;
			references.add(referencetv);
			jdbconn.insert(references, "reference" + Utils.getDay(timestamp));
		}

	}

	public static String setNull(String str) {
		if (str == null)
			return "";
		return str;
	}

	public static String getAlbum(String source) {
		if (source == null || source.isEmpty())
			return null;
		int index = 0;
		String album = null;
		if (source != null || !source.equals("")) {
			if (source.indexOf("title") > 0) {
				index = source.indexOf("<title>");
				source = source.substring(index);
				if (source.indexOf("-") > 0) {
					album = source.substring(7, source.indexOf("-"));
				}
			}
		}
		return album;
	}

	public void zongyiInfoParser(String[] arrs, String content,
			JDBCConnection jdbconn) {
		// arrs: sh rowKey ,info ,url,date, category
		// source mark zhishu playcount
		if (content == null || content.equals(""))
			return;
		ArrayList<String> dyn = new ArrayList<String>();
		dyn = getDynamics(content);
		String category = arrs[5];
		if (dyn.size() < 4)
			return;
		for (int j = 0; j < dyn.size(); j++) {
			if (dyn.get(j).indexOf("502 Bad Gateway") >= 0) {
				jdbconn.log("李辉", arrs[1], 1, "sh", arrs[1], "播放量没有抓到", 2);	
				return;
			}
		}
		String source = dyn.get(0);
		String markstr2 = dyn.get(1);
		String zhishustr3 = dyn.get(2);
		double markTotal = 0.0;
		double peopleTotal = 0.0;
		double[] marks = new double[2];
		marks = Utils.getMarks(dyn.get(1));
		markTotal = marks[1];
		peopleTotal = marks[0];
		String playcount = Utils.getInfoCount(dyn.get(3));
		String[] baseInfoGroup = zongyiBaseInfo(source, arrs[3]);
		if (baseInfoGroup == null)
			return;
		for (int j = 0; j < baseInfoGroup.length; j++) {
			if (baseInfoGroup[j] == null)
				baseInfoGroup[j] = "";
		}
		int index = 0;

		if (zhishustr3.indexOf("indexValue") < 0)
			return;
		zhishustr3 = zhishustr3
				.substring(zhishustr3.indexOf("indexValue") + 13);
		if (zhishustr3.indexOf("\"") < 0)
			return;
		String zhishu = zhishustr3.substring(0, zhishustr3.indexOf("\""));
		zhishu = zhishu.replace("\\u002e", ".");
		String timestamp = null;
		timestamp = arrs[4].substring(0,10);
		String videoinfokey = arrs[1] + "+sh";
		String inforowkey = arrs[1];
		String[] videoinforows = { videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey };
		String[] videoinfocolfams = { "R", "B", "R", "B", "C", "C", "C", "C",
				"B", "C", "R" };
		String[] videoinfoquals = { "website", "url", "inforowkey", "name",
				"host", "playtv", "area", "categories", "category",
				"summarize", "timestamp" };
		String[] videoinfovalues = { "sh", arrs[3], inforowkey,
				baseInfoGroup[0], baseInfoGroup[1], baseInfoGroup[2],
				baseInfoGroup[3], baseInfoGroup[4], arrs[5], baseInfoGroup[5],
				timestamp };
		for (int j = 0; j < videoinfovalues.length; j++) {
			videoinfovalues[j] = setNull(videoinfovalues[j]);
		}
		try {
			hbase.putRows("movieinfosh", videoinforows, videoinfocolfams,
					videoinfoquals, videoinfovalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// mysql

		ArrayList<TextValue> valuesinfo = new ArrayList<TextValue>();

		TextValue rowkey = new TextValue();
		TextValue moviename = new TextValue();
		TextValue categoryString = new TextValue();
		TextValue website = new TextValue();
	
	
		website.text = "website";
		website.value = "sh";
		valuesinfo.add(website);
	
		categoryString.text = "category";
		categoryString.value = arrs[5];
		valuesinfo.add(categoryString);
		
		rowkey.text = "rowkey";
		rowkey.value = inforowkey;
		valuesinfo.add(rowkey);
	
		moviename.text = "moviename";
		moviename.value = baseInfoGroup[0];
		valuesinfo.add(moviename);
		TextValue timestampString = new TextValue();
		timestampString.text = "crawltime";
		timestampString.value = Utils.getDay(timestamp);
		valuesinfo.add(timestampString);
		TextValue area = new TextValue();
		area.text = "area";
		area.value = baseInfoGroup[3];
		valuesinfo.add(area);
		String[] typeSplits = baseInfoGroup[4].split("@");
		for (int i = 0; i < typeSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i + 1);
			typetv.value = typeSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		String[] hostSplits = baseInfoGroup[1].split("@");
		for (int i = 0; i < hostSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "host" + (i + 1);
			typetv.value = hostSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		TextValue playtv = new TextValue();
		playtv.text = "playtv";
		playtv.value = baseInfoGroup[2];
		valuesinfo.add(playtv);
		TextValue summarize = new TextValue();
		summarize.text = "summarize";
		summarize.value = baseInfoGroup[5];
		valuesinfo.add(summarize);
		if (infoIsExist(inforowkey, "movie") == 0) {
		jdbconn.insert(valuesinfo, "movieinfo");
		}
		
		String videodynamickey = arrs[1] + "+sh+" + timestamp;
		String[] videodynamicrows = { videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey };
		String[] videodynamiccolfams = { "R", "R", "R", "B", "C", "C", "C",
				"C", "B" };
		String[] videodynamicquals = { "website", "inforowkey", "timestamp",
				"name", "sumplaycount", "markTotal", "peopleTotal", "zhishu",
				"category" };
		String[] videodynamicvalues = { "sh", arrs[1], timestamp,
				baseInfoGroup[0], playcount, markTotal + "", peopleTotal + "",
				zhishu, category };
		for (int j = 0; j < videodynamicvalues.length; j++) {
			videodynamicvalues[j] = setNull(videodynamicvalues[j]);
		}
		try {
			hbase.putRows("moviedynamicsh", videodynamicrows,
					videodynamiccolfams, videodynamicquals, videodynamicvalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// mysql

		ArrayList<TextValue> valuesdynamic = new ArrayList<TextValue>();
		// rowkey website category movieName timestamp sumPlayCount peopelNum
		// score free
		valuesdynamic.add(rowkey);
		valuesdynamic.add(website);
		valuesdynamic.add(categoryString);
		TextValue movieName = new TextValue();
		movieName.text = "movieName";
		movieName.value = baseInfoGroup[0];
		valuesdynamic.add(movieName);
		TextValue timestampdynamic = new TextValue();
		timestampdynamic.text = "timestamp";
		timestampdynamic.value = timestamp;
		valuesdynamic.add(timestampdynamic);
		TextValue sumPlayCount = new TextValue();
		sumPlayCount.text = "sumPlayCount";
		sumPlayCount.value = Utils.ConvertToInt(playcount);
		valuesdynamic.add(sumPlayCount);
		TextValue peopelNum = new TextValue();
		peopelNum.text = "peopleNum";
		peopelNum.value = peopleTotal;
		valuesdynamic.add(peopelNum);
		TextValue score = new TextValue();
		score.text = "score";
		score.value = markTotal;
		valuesdynamic.add(score);
		TextValue zhishuString = new TextValue();
		zhishuString.text = "zhishu";
		zhishuString.value = Utils.ConvertToDouble(zhishu);
		valuesdynamic.add(zhishuString);
		jdbconn.insert(valuesdynamic, "moviedynamic" + Utils.getDay(timestamp));

	}


	public void ysdmInfoParser(String[] arrs, String content,
			JDBCConnection jdbconn) {

		// arrs: sh rowKey,info,url,date, category,
		// source mark zhishu playcount
		// 1-27
		if (content == null || content.equals(""))
			return;
		ArrayList<String> dyn = new ArrayList<String>();
		dyn = getDynamics(content);
		if (dyn.size() < 4)
			return;
		for (int j = 0; j < dyn.size(); j++) {
			if (dyn.get(j).indexOf("502 Bad Gateway") >= 0) {
				jdbconn.log("李辉", arrs[1], 1, "sh", arrs[1], "播放量没有抓到", 2);	
				return;
			}
		}
		String category = arrs[5];
		String source = dyn.get(0);
		String zhishustr3 = dyn.get(2);
		String playcount = "", zhishu = "";
		double markTotal = 0.0;
		double peopleTotal = 0.0;
		double[] marks = new double[2];
		marks = Utils.getMarks(dyn.get(1));
		markTotal = marks[1];
		peopleTotal = marks[0];
		playcount = Utils.getInfoCount(dyn.get(3));
		if (zhishustr3.indexOf("indexValue") < 0)
			return;
		zhishustr3 = zhishustr3
				.substring(zhishustr3.indexOf("indexValue") + 13);
		if (zhishustr3.indexOf("\"") < 0)
			return;
		zhishu = zhishustr3.substring(0, zhishustr3.indexOf("\""));
		zhishu = zhishu.replace("\\u002e", ".");
		String[] baseInfoGroup = new String[8];
		baseInfoGroup = ysdmBaseInfo0(content, arrs[3]);
		if (baseInfoGroup == null)
			baseInfoGroup = ysdmBaseInfo1(content, arrs[3]);
		if (baseInfoGroup == null)
			return;
		for (int j = 0; j < baseInfoGroup.length; j++) {
			if (baseInfoGroup[j] == null)
				baseInfoGroup[j] = "";
		}

		String timestamp = null;
		timestamp = arrs[4].substring(0,10);
		String videoinfokey = arrs[1] + "+sh";
		String inforowkey = arrs[1];
		String[] videoinforows = { videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey };
		String[] videoinfocolfams = { "R", "R", "B", "R", "B", "C", "C", "C",
				"C", "C", "B", "C" };
		String[] videoinfoquals = { "website", "timestamp", "url",
				"inforowkey", "name", "director", "mainactor", "summarize",
				"publishYear", "categories", "category", "area" };
		String[] videoinfovalues = { "sh", timestamp, arrs[3], inforowkey,
				baseInfoGroup[0], baseInfoGroup[1], baseInfoGroup[2],
				baseInfoGroup[3], baseInfoGroup[4], baseInfoGroup[5], arrs[5],
				baseInfoGroup[6] };
		// Halbum, Hdirector, Hactor, Hintro, Hyear, Htype
		for (int j = 0; j < videoinfovalues.length; j++) {
			videoinfovalues[j] = setNull(videoinfovalues[j]);
		}

		try {
			hbase.putRows("movieinfosh", videoinforows, videoinfocolfams,
					videoinfoquals, videoinfovalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// mysql
		ArrayList<TextValue> valuesinfo = new ArrayList<TextValue>();
		TextValue categoryString = new TextValue();
		TextValue website = new TextValue();
		TextValue rowkey = new TextValue();
		TextValue moviename = new TextValue();
		
		
		website.text = "website";
		website.value = "sh";
		valuesinfo.add(website);
		
		categoryString.text = "category";
		categoryString.value = arrs[5];
		valuesinfo.add(categoryString);
	
		rowkey.text = "rowkey";
		rowkey.value = inforowkey;
		valuesinfo.add(rowkey);
		
		moviename.text = "moviename";
		moviename.value = baseInfoGroup[0];
		valuesinfo.add(moviename);
		TextValue timestampString = new TextValue();
		timestampString.text = "crawltime";
		timestampString.value = Utils.getDay(timestamp);
		valuesinfo.add(timestampString);
		TextValue publishYear = new TextValue();
		publishYear.text = "year";
		publishYear.value = baseInfoGroup[4];
		valuesinfo.add(publishYear);
		TextValue area = new TextValue();
		area.text = "area";
		area.value = baseInfoGroup[6];
		valuesinfo.add(area);
		String[] typeSplits = baseInfoGroup[5].split("@");
		for (int i = 0; i < typeSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i + 1);
			typetv.value = typeSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}

		String[] directorSplits = baseInfoGroup[1].split("@");
		for (int i = 0; i < directorSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "director" + (i + 1);
			typetv.value = directorSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		String[] actorSplits = baseInfoGroup[2].split("@");
		for (int i = 0; i < actorSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "mainactor" + (i + 1);
			typetv.value = actorSplits[i];
			valuesinfo.add(typetv);
			if (i == 2)
				break;
		}
		TextValue summarize = new TextValue();
		summarize.text = "summarize";
		summarize.value = baseInfoGroup[3];
		valuesinfo.add(summarize);
		if (infoIsExist(inforowkey, "movie") == 0) {
		jdbconn.insert(valuesinfo, "movieinfo");		
		}

		String videodynamickey = arrs[1] + "+sh+" + timestamp;

		String[] videodynamicrows = { videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey };
		String[] videodynamiccolfams = { "R", "R", "R", "B", "C", "C", "C",
				"C", "B" };
		String[] videodynamicquals = { "website", "inforowkey", "timestamp",
				"name", "sumplaycount", "markTotal", "peopleTotal", "zhishu",
				"category" };
		String[] videodynamicvalues = { "sh", arrs[1], timestamp,
				baseInfoGroup[0], playcount, markTotal + "", peopleTotal + "",
				zhishu, category };
		for (int j = 0; j < videodynamicvalues.length; j++) {
			videodynamicvalues[j] = setNull(videodynamicvalues[j]);
		}
		try {
			hbase.putRows("moviedynamicsh", videodynamicrows,
					videodynamiccolfams, videodynamicquals, videodynamicvalues);

		} catch (Exception e) {
			e.printStackTrace();
		}
		// mysql
		ArrayList<TextValue> valuesdynamic = new ArrayList<TextValue>();
		valuesdynamic.add(rowkey);
		valuesdynamic.add(website);
		valuesdynamic.add(categoryString);
		TextValue movieName = new TextValue();
		movieName.text = "movieName";
		movieName.value = baseInfoGroup[0];
		valuesdynamic.add(movieName);
		TextValue timestampdynamic = new TextValue();
		timestampdynamic.text = "timestamp";
		timestampdynamic.value = timestamp;
		valuesdynamic.add(timestampdynamic);
		TextValue sumPlayCount = new TextValue();
		sumPlayCount.text = "sumPlayCount";
		sumPlayCount.value = Utils.ConvertToInt(playcount);
		valuesdynamic.add(sumPlayCount);
		TextValue peopelNum = new TextValue();
		peopelNum.text = "peopleNum";
		peopelNum.value = peopleTotal;
		valuesdynamic.add(peopelNum);
		TextValue score = new TextValue();
		score.text = "score";
		score.value = markTotal;
		valuesdynamic.add(score);
		TextValue zhishutv = new TextValue();
		zhishutv.text = "zhishu";
		zhishutv.value = Utils.ConvertToDouble(zhishu);
		valuesdynamic.add(zhishutv);
		jdbconn.insert(valuesdynamic, "moviedynamic" + Utils.getDay(timestamp));

	}


	public String getMW(String source, String target) {
		String c_content = null;
		c_content = source;
		int v_index = 0;
		String href = null;
		if (c_content.indexOf(target) < 0)
			return null;
		if (c_content.indexOf(target) > c_content.indexOf("remark-count"))
			return null;
		c_content = c_content.substring(c_content.indexOf(target),
				c_content.indexOf("remark-count"));// gai zhongwen
		c_content = c_content.substring(c_content
				.indexOf(" <div class=\"pic\">") + 12);
		v_index = c_content.indexOf("href");
		StringBuffer memRec = new StringBuffer();
		href = null;
		String name = null;
		while (v_index > -1) {
			String temp = null;
			int index = 0;
			index = c_content.indexOf("<img");
			if (index < 0)
				break;
			temp = c_content.substring(0, index + 4);
			if (temp.indexOf("title=\"") - 2 > v_index + 6)
				href = temp
						.substring(v_index + 6, temp.indexOf("title=\"") - 2);
			name = temp.substring(temp.indexOf("title=\"") + 7,
					c_content.indexOf("<img") - 2);
			memRec.append(name + "$" + href + "@");
			index = c_content.indexOf(" <div class=\"pic\">");
			if (index < 0)
				break;
			c_content = c_content.substring(c_content
					.indexOf(" <div class=\"pic\">") + 12);

			v_index = c_content.indexOf("href");

		}
		c_content = memRec.substring(0, memRec.lastIndexOf("@"));
		memRec = new StringBuffer(c_content);
		return c_content;
	}

	public String getWstb(String source, String target) {
		String c_content = null;
		c_content = source;
		int v_index = 0;
		String href = null;
		c_content = c_content.substring(c_content.indexOf(target),
				c_content.indexOf("remark-count"));// gai zhongwen
		c_content = c_content.substring(c_content
				.indexOf(" <div class=\"pic\">") + 12);
		v_index = c_content.indexOf("href");
		StringBuffer memRec = new StringBuffer();
		href = null;
		while (v_index > -1) {
			String temp = null;
			int index = 0;
			index = c_content.indexOf("<img");
			if (index < 0)
				break;
			temp = c_content.substring(0, index);
			href = temp.substring(v_index + 6, temp.indexOf("title=\"") - 2);
			memRec.append(href + "@");
			index = c_content.indexOf(" <div class=\"pic\">");
			if (index < 0)
				break;
			c_content = c_content.substring(c_content
					.indexOf(" <div class=\"pic\">") + 12);

			v_index = c_content.indexOf("href");

		}
		c_content = memRec.substring(0, memRec.lastIndexOf("@"));
		memRec = new StringBuffer(c_content);
		return c_content;
	}


	public void getPaidParser(String[] arrs, String content,
			JDBCConnection jdbconn) {
		// arrs :sh rowkey info/play url time category infokey
		// source dingcai comm play
		ArrayList<String> dyn = new ArrayList<String>();
		dyn = getDynamics(content);
		if (dyn.size() < 4)
			return;
		for (int j = 0; j < dyn.size(); j++) {
			if (dyn.get(j).indexOf("502 Bad Gateway") >= 0)	{
				jdbconn.log("李辉", arrs[6], 1, "sh", arrs[1], "播放量没有抓到", 2);	
				return;
			}
		}

		String[] dcGroup = new String[2];
		String comm = null;
		comm = Utils.getComm(dyn.get(2));
		String albumName = getAlbum(dyn.get(0));
		dcGroup = getDC(dyn.get(1));
		String totalCount = dyn.get(3);
		String timestamp = null;
		timestamp = arrs[4].substring(0,10);
		String videoinfokey = arrs[6] + "+" + arrs[1] + "+sh";
		String showtype = "付费";
		// inforowkey+playrowkey+"sh"
		String[] videoinforows = { videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey };
		String[] videoinfocolfams = { "R", "B", "R", "R", "C", "B", "R", "B" };
		String[] videoinfoquals = { "website", "url", "playrowkey",
				"inforowkey", "comment", "showtype", "timestamp", "name" };
		String[] videoinfovalues = { "sh", arrs[3], arrs[1], arrs[6], comm,
				showtype, timestamp, albumName };
		for (int j = 0; j < videoinfovalues.length; j++) {
			videoinfovalues[j] = setNull(videoinfovalues[j]);
		}
		try {
		
			hbase.putRows("videoinfosh", videoinforows, videoinfocolfams,
					videoinfoquals, videoinfovalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		TextValue website = new TextValue();
		TextValue rowkey = new TextValue();
		TextValue inforowkey = new TextValue();
		TextValue playrowkeytv = new TextValue();
		TextValue commenttv = new TextValue();
	
		ArrayList<TextValue> valuesinfo = new ArrayList<TextValue>();
	
		website.text = "website";
		website.value = "sh";
		valuesinfo.add(website);
	
		rowkey.text = "rowkey";
		rowkey.value = arrs[6];
		valuesinfo.add(rowkey);
	
		inforowkey.text = "inforowkey";
		inforowkey.value = arrs[6];
		valuesinfo.add(inforowkey);
	
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = arrs[1];
		valuesinfo.add(playrowkeytv);
		TextValue moviename = new TextValue();
		moviename.text = "name";
		moviename.value = albumName;
		valuesinfo.add(moviename);
		TextValue timestampString = new TextValue();
		timestampString.text = "crawltime";
		timestampString.value = Utils.getDay(timestamp);
		valuesinfo.add(timestampString);
		
		int commentInt = Utils.ConvertToInt(comm);
		commenttv.text = "comment";
		commenttv.value = commentInt;
		valuesinfo.add(commenttv);
		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = showtype;
		valuesinfo.add(showtypetv);
		if (infoIsExist(arrs[6], "video") == 0) {
		jdbconn.insert(valuesinfo, "videoinfo");
		}
		
		
		String videodynamickey = arrs[6] + "+" + arrs[1] + "+sh+" + timestamp;
		String[] videodynamicrows = { videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey };
		String[] videodynamiccolfams = { "R", "R", "R", "R", "C", "C", "C", "C" };
		String[] videodynamicquals = { "website", "playrowkey", "inforowkey",
				"timestamp", "ding", "cai", "sumplaycount", "comment" };
		String[] videodynamicvalues = { "sh", arrs[1], arrs[6], timestamp,
				dcGroup[0], dcGroup[1], totalCount, comm };
		for (int j = 0; j < videodynamicvalues.length; j++) {
			videodynamicvalues[j] = setNull(videodynamicvalues[j]);
		}
		try {
			hbase.putRows("videodynamicsh", videodynamicrows,
					videodynamiccolfams, videodynamicquals, videodynamicvalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
		ArrayList<TextValue> valuesdynamic = new ArrayList<TextValue>();
		valuesdynamic.add(website);
		valuesdynamic.add(inforowkey);
		valuesdynamic.add(playrowkeytv);
		valuesdynamic.add(rowkey);
		valuesdynamic.add(commenttv);
		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = timestamp;
		valuesdynamic.add(timestamptv);
		int dingInt = Utils.ConvertToInt(dcGroup[0]);
		TextValue dingtv = new TextValue();
		dingtv.text = "up";
		dingtv.value = dingInt;
		valuesdynamic.add(dingtv);
		int caiInt = Utils.ConvertToInt(dcGroup[1]);
		TextValue caitv = new TextValue();
		caitv.text = "down";
		caitv.value = caiInt;
		valuesdynamic.add(caitv);
		TextValue sumplaycounttv = new TextValue();
		sumplaycounttv.text = "sumplaycount";
		sumplaycounttv.value = Utils.ConvertToInt(totalCount);
		valuesdynamic.add(sumplaycounttv);
		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;
		valuesdynamic.add(flagtv);
		jdbconn.insert(valuesdynamic, "videodynamic" + Utils.getDay(timestamp));

	}

	
	public static String[] parseMovieBaseinfo(String source) {
		if (source == null || source.equals(""))
			return null;
		Pattern p = Pattern.compile("movie-infoR.*mod relNews hide");
		Matcher m = p.matcher(source);
		String[] baseinfos = new String[7];
		if (m.find()) {
			Pattern p1 = Pattern.compile("(<li.{5,}?</li>)");
			Matcher m1 = p1.matcher(m.group());
			int index = 0;
			while (m1.find()) {
				index++;
		//		System.out.println(m1.group());
					if (m1.group().indexOf("上映时间") > 0) {
						baseinfos[0] = Utils.getYear(m1.group());
					}
					else if(m1.group().indexOf("地区")>0) {
						baseinfos[1] = Utils.getArea(m1.group());
					}
					else if(m1.group().indexOf("类型")>0) {
						baseinfos[2] = Utils.getArea(m1.group());
						}
					else if(m1.group().indexOf("导演")>0) {
						baseinfos[3] = Utils.getDirector(m1.group());
						}				    
					else if(m1.group().indexOf("主演")>0) {
						baseinfos[4] = Utils.getDirector(m1.group());
						}	
					else if(m1.group().indexOf("片长")>0) {
						baseinfos[5] = Utils.getMinute(m1.group());
						}
					else if(m1.group().indexOf("评分")>0) {
						baseinfos[6] = Utils.getscore(m1.group());
						}
				}
				}
		
		return baseinfos;

	}

	public void dyPlayParser(String[] arrs, String content,JDBCConnection jdbconn) {
		ArrayList<String> dyn = new ArrayList<String>();
		dyn = getDynamics(content);
		if (dyn.size() < 6)
			return;
		for (int j = 0; j < dyn.size(); j++) {
			if (dyn.get(j).indexOf("502 Bad Gateway") >= 0)
				return;
		}
		String[] dcGroup = new String[2];
		String rec = null, simi = null, memRec = null, comm = null;
		if (arrs[5].equals("dianyingyugao")
				&& arrs[3].indexOf("v.tv.sohu.com") >= 0)
			return;
		comm = Utils.getComm(dyn.get(3));
		dcGroup = getDC(dyn.get(1));
		String[] play = Utils.splitNumber(dyn.get(2), "vids");
		if (play == null) {
			jdbconn.log("李辉",arrs[6], 1, "sh", arrs[1], "播放量没有抓到", 2);		
			return;
		}
		String totalCount = play[0];
		String todayCount = play[1];
		rec = getRec(dyn.get(5));
		simi = getSimi(dyn.get(4));
		if (arrs[5].indexOf("yugao") < 0)
			memRec = "";
		else
			memRec = getMW(dyn.get(0), "list list-140 cfix h524");
		String albumName = null;
		albumName = getAlbum(content);
		String timestamp = null;
		timestamp = arrs[4].substring(0,10);;
		String videoinfokey = arrs[6] + "+" + arrs[1] + "+sh";
		String showtype = null;
		if (arrs[5].indexOf("zhengpian") >= 0)
			showtype = "正片";
		else if (arrs[5].indexOf("yugao") >= 0)
			showtype = "预告片";
		// inforowkey+playrowkey+"sh"
		String[] videoinforows = { videoinfokey, videoinfokey, videoinfokey,
				videoinfokey, videoinfokey, videoinfokey, videoinfokey,
				videoinfokey };
		String[] videoinfocolfams = { "R", "B", "R", "R", "B", "C", "B", "R" };
		String[] videoinfoquals = { "website", "url", "playrowkey",
				"inforowkey", "name", "comment", "showtype", "timestamp" };
		String[] videoinfovalues = { "sh", arrs[3], arrs[1], arrs[6],
				albumName, comm, showtype, timestamp };
		for (int j = 0; j < videoinfovalues.length; j++) {
			videoinfovalues[j] = setNull(videoinfovalues[j]);
		}
		try {
			hbase.putRows("videoinfosh", videoinforows, videoinfocolfams,
					videoinfoquals, videoinfovalues);
		} catch (Exception e) {
			e.printStackTrace();
		}

		TextValue website = new TextValue();
		TextValue rowkey = new TextValue();
		TextValue inforowkey = new TextValue();
		TextValue playrowkeytv = new TextValue();
		TextValue commenttv = new TextValue();
	
		ArrayList<TextValue> valuesinfo = new ArrayList<TextValue>();
	
		website.text = "website";
		website.value = "sh";
		valuesinfo.add(website);
		
		rowkey.text = "rowkey";
		rowkey.value = arrs[6];
		valuesinfo.add(rowkey);
	
		inforowkey.text = "inforowkey";
		inforowkey.value = arrs[6];
		valuesinfo.add(inforowkey);
	
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = arrs[1];
		valuesinfo.add(playrowkeytv);
		TextValue moviename = new TextValue();
		moviename.text = "name";
		moviename.value =albumName;
		valuesinfo.add(moviename);
		TextValue timestampString = new TextValue();
		timestampString.text = "crawltime";
		timestampString.value =Utils.getDay(timestamp);
		valuesinfo.add(timestampString);
	
		int commentInt= Utils.ConvertToInt(comm);
		commenttv.text = "comment";
		commenttv.value = commentInt;
		valuesinfo.add(commenttv);
		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = showtype;
		valuesinfo.add(showtypetv);
		if (infoIsExist(arrs[6], "video") == 0) { 
		jdbconn.insert(valuesinfo, "videoinfo");
		}
		
		
		String videodynamickey = arrs[6] + "+" + arrs[1] + "+sh+" + timestamp;
		String[] videodynamicrows = { videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey, videodynamickey, videodynamickey,
				videodynamickey };
		String[] videodynamiccolfams = { "R", "R", "R", "R", "C", "C", "C",
				"C", "C", "C", "C", "C" };
		String[] videodynamicquals = { "website", "playrowkey", "inforowkey",
				"timestamp", "ding", "cai", "sumplaycount", "todaycount",
				"rec", "simi", "memRec", "comment" };
		String[] videodynamicvalues = { "sh", arrs[1], arrs[6], timestamp,
				dcGroup[0], dcGroup[1], totalCount, todayCount, rec, simi,
				memRec, comm };
		for (int j = 0; j < videodynamicvalues.length; j++) {

			videodynamicvalues[j] = setNull(videodynamicvalues[j]);
		}
		try {
			hbase.putRows("videodynamicsh", videodynamicrows,
					videodynamiccolfams, videodynamicquals, videodynamicvalues);
		} catch (Exception e) {
			e.printStackTrace();
		}
//mysql
	
		ArrayList<TextValue> valuesdynamic = new ArrayList<TextValue>();
		valuesdynamic.add(website);
		valuesdynamic.add(inforowkey);
		valuesdynamic.add(playrowkeytv);
		valuesdynamic.add(rowkey);
		valuesdynamic.add(commenttv);
		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value =timestamp;
		valuesdynamic.add(timestamptv);
		int dingInt=Utils.ConvertToInt(dcGroup[0]);
		TextValue dingtv = new TextValue();
		dingtv.text = "up";
		dingtv.value = dingInt;
		valuesdynamic.add(dingtv);
		int caiInt=Utils.ConvertToInt(dcGroup[1]);
		TextValue caitv = new TextValue();
		caitv.text = "down";
		caitv.value = caiInt;
		valuesdynamic.add(caitv);
		TextValue sumplaycounttv = new TextValue();
		sumplaycounttv.text = "sumplaycount";
		sumplaycounttv.value = Utils.ConvertToInt(totalCount);
		valuesdynamic.add(sumplaycounttv);
		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;
		valuesdynamic.add(flagtv);
		//--hualili
	jdbconn.insert(valuesdynamic, "videodynamic"+Utils.getDay(timestamp));
	if(simi!=null) {
	String[] simiParts = simi.split("@");
	if (simiParts == null )
		return;
	for (int j = 0; j < simiParts.length; j++) {
		String[] pairs = simiParts[j].split("\\$");
		if (pairs == null || pairs.length < 2)
			return;
		ArrayList<TextValue> references = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = Utils.getRowkey(pairs[1]);
		references.add(rowkeytv);
		TextValue websitet = new TextValue();
		websitet.text = "website";
		websitet.value ="sh";
		references.add(websitet);
		TextValue referencetv = new TextValue();
		referencetv.text = "reference";
		referencetv.value = 1;
		references.add(referencetv);
		jdbconn.insert(references, "reference" + Utils.getDay(timestamp));
	}
	}


	}

	public String[] getDC(String dcContent) {
		if (dcContent == null || dcContent.equals(""))
			return null;
		String[] dc = new String[2];
		if (dcContent.indexOf("upCount") < 0
				|| dcContent.indexOf("downCount") < 0)
			return null;
		dcContent = dcContent.substring(dcContent.indexOf("upCount") + 9);
		dc[0] = dcContent.substring(0, dcContent.indexOf(","));
		dc[1] = dcContent.substring(dcContent.indexOf("downCount"
				+ "") + 11,
				dcContent.indexOf("}"));
		return dc;
	}

	public String getPlayCountInfo(String playcContent, String source) {
		if (playcContent == null || playcContent.equals(""))
			return null;
		String playcount = null;
		if (playcContent.indexOf("count") < 0)
			return getPlayCountSrc(source);
		playcount = playcContent.substring(playcContent.indexOf("count") + 6,
				playcContent.indexOf(";"));
		if (playcount.equals("0"))
			return getPlayCountSrc(source);
		return playcount;
	}

	public String getPlayCountPlay(String playcContent) {
		if (playcContent == null || playcContent.equals(""))
			return null;
		String playcount = null;
		int index1 = 0;
		int index2 = 0;
		index1 = playcContent.indexOf("count") + 6;
		index2 = playcContent.indexOf(";");
		if (index1 < 0 || index2 < 0 || index1 > index2)
			return null;
		playcount = playcContent.substring(index1, index2);
		return playcount;
	}

	public String getPlayCountSrc(String source) {
		if (source == null || source.equals(""))
			return null;
		int index = 0;
		String temp = source;
		index = temp.indexOf("总播放");
		if (index < 0)
			return null;
		temp = temp.substring(index + 4);
		index = temp.indexOf("</span>");
		if (index < 0)
			return null;
		temp = temp.substring(0, index);
		return temp;
	}

	public String getRec(String recContent) {
		if (recContent == null || recContent.equals(""))
			return null;
		StringBuffer rec = new StringBuffer();
		String temprec = null;
		String tempUrl = null;
		String tempTitle = null;
		int v_index = 0;
		v_index = recContent.indexOf("title");
		if (v_index < 0)
			return null;
		while (v_index >= 0) {
			recContent = recContent.substring(v_index + 8);
			v_index = recContent.indexOf("\"");
			if (v_index < 0)
				return null;
			tempTitle = recContent.substring(0, v_index);
			v_index = recContent.indexOf("link");
			recContent = recContent.substring(v_index + 7);
			if (v_index < 0)
				return null;
			v_index = recContent.indexOf("html");
			tempUrl = recContent.substring(0, v_index + 4);
			rec.append(tempTitle + "$" + tempUrl + "@");
			v_index = recContent.indexOf("title");
		}
		if (rec.lastIndexOf("@") > 0)
			temprec = rec.substring(0, rec.lastIndexOf("@"));
		return temprec;
	}

	public String getSimi(String simiContent) {
		if (simiContent == null || simiContent.equals(""))
			return null;
		StringBuffer simi = new StringBuffer();
		String tempsimi = null;
		int v_index = 0;
		v_index = simiContent.indexOf("videoName");
		while (v_index >= 0) {
			simiContent = simiContent.substring(v_index + 12);
			v_index = simiContent.indexOf("videoMainActor");
			if (v_index < 0)
				break;
			tempsimi = simiContent.substring(0,
					simiContent.indexOf("videoMainActor") - 3);
			tempsimi = convert(tempsimi);
			simi.append(tempsimi + "$");
			v_index = simiContent.indexOf("videoUrl");
			if (v_index < 0)
				break;
			simiContent = simiContent.substring(v_index + 11);
			v_index = simiContent.indexOf("html");
			if (v_index < 0)
				break;
			tempsimi = simiContent
					.substring(0, simiContent.indexOf("html") + 4);
			// tempsimi = convert(tempsimi);
			simi.append(tempsimi + "@");
			v_index = simiContent.indexOf("videoName");
		}
		if (simi.lastIndexOf("@") > 0)
			tempsimi = simi.substring(0, simi.lastIndexOf("@"));
		return tempsimi;
	}

	public String getOtherSimi(String simiContent) {
		if (simiContent == null || simiContent.equals(""))
			return null;
		StringBuffer simi = new StringBuffer();
		String tempsimi = null;
		int v_index = 0;
		v_index = simiContent.indexOf("videoName");
		while (v_index >= 0) {
			simiContent = simiContent.substring(v_index + 12);
			v_index = simiContent.indexOf("videoPlayTime");
			if (v_index < 0)
				break;
			tempsimi = simiContent.substring(0,
					simiContent.indexOf("videoPlayTime") - 3);
			tempsimi = convert(tempsimi);
			simi.append(tempsimi + "$");
			v_index = simiContent.indexOf("videoUrl");
			if (v_index < 0)
				break;
			simiContent = simiContent.substring(v_index + 11);
			v_index = simiContent.indexOf("html");
			if (v_index < 0)
				break;
			tempsimi = simiContent
					.substring(0, simiContent.indexOf("html") + 4);
			// tempsimi = convert(tempsimi);
			simi.append(tempsimi + "@");
			v_index = simiContent.indexOf("videoName");
		}
		if (simi.lastIndexOf("@") > 0)
			tempsimi = simi.substring(0, simi.lastIndexOf("@"));
		return tempsimi;
	}

	public String convert(String utfString) {
		StringBuilder sb = new StringBuilder();
		int i = -1;
		int pos = 0;

		while ((i = utfString.indexOf("\\u", pos)) != -1) {
			sb.append(utfString.substring(pos, i));
			if (i + 5 < utfString.length()) {
				pos = i + 6;
				sb.append((char) Integer.parseInt(
						utfString.substring(i + 2, i + 6), 16));
			}
		}

		return sb.toString();
	}

	public int infoIsExist(String rowkey, String tabletype) {
		int count = 0;
		ResultSet rs = jdbconn.executeQuerySingle("select count(*) from "
				+ tabletype + "info where rowkey = '" + rowkey + "'");
		try {
			while (rs.next()) {
				String cou = rs.getString(1);
				count = Integer.parseInt(cou);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return count;
	}
	
	
}
