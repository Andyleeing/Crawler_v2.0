package ParserData.SohuParserData;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
	public static String getComm(String comm) {
		if (comm == null || comm.equals(""))
			return null;
		int index = 0;
		index = comm.indexOf("allCount");
		if (index < 0)
			return null;
		String temp = comm;
		temp = temp.substring(index + 10);
		index = temp.indexOf(",");
		if (index < 0)
			return null;
		temp = temp.substring(0, index);
		if (temp == null)
			temp = "";
		return temp;
	}
 
	public static String getYear(String yearString) {
		if (yearString == null || yearString.equals(""))
			return null;
		Pattern p = Pattern.compile("\\d{4}");
		Matcher m = p.matcher(yearString);
		if (m.find())
			return m.group();
		else
			return "";

	}
	
	public static String getMinute(String yearString) {
		if (yearString == null || yearString.equals(""))
			return null;
		Pattern p = Pattern.compile("\\d*分");
		Matcher m = p.matcher(yearString);
		if (m.find())
			return m.group().replaceAll("分","");
		else
			return null;

	}
	public static String getArea(String areaString){
		if (areaString == null || areaString.equals(""))return null;
		String tempArea="";
		Pattern p = Pattern.compile("(work_search_vv\">)(.{1,}?)(</a>)");
		Matcher m = p.matcher(areaString);
		while(m.find()){
			if(tempArea=="")tempArea=m.group(2);
			else tempArea=tempArea+"@"+m.group(2);
		}
		return tempArea;
	}
	
	public static String getDay(String timestamp){
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Long longday=new Long(timestamp+"000");
		String day=sdf.format(longday);
		return day;
	}
	public static String getDirector(String areaString){
		if (areaString == null || areaString.equals(""))return null;
		String tempArea="";
		Pattern p = Pattern.compile("(work_info_others\">)(.{1,}?)(</a>)");
		Matcher m = p.matcher(areaString);
		while(m.find()){
			if(tempArea=="")tempArea=m.group(2);
			else tempArea=tempArea+"@"+m.group(2);
		}
		return tempArea;
	}

	public static String getRowkey(String url){
		if(url==null||url.equals(""))return null;
		Pattern p= Pattern.compile("(http://tv.sohu.com/)(.*)");
		Matcher m=p.matcher(url);
		if(m.find()){
	    return m.group(2);
		}
		return "";
	}
	
	public static String getscore(String areaString){
		if (areaString == null || areaString.equals(""))return null;
		Pattern p = Pattern.compile("(score\">)(.{1,}?)(</strong>)");
		Matcher m = p.matcher(areaString);
	if(m.find()){
	  return m.group(2);
		}
	  return "";
	}
	public static double ConvertToDouble(String str){
		double value=-1;
		try {
			value = Double.parseDouble(str);
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
	public static double[] getMarks(String markString){
		double[]marks=new double[2];
		if(markString==null||markString.equals(""))return marks;
		double d1=0.0,d2=0.0;
		d1=parseDigit(markString,"totalScore_");
		d2=parseDigit(markString,"votecount_");
		if(d1==0.0||d2==0.0)return marks;
		else {
			marks[0]=d2;
			marks[1]=d1/d2;
		}
		return marks;
	}
	public static double parseDigit(String digitString,String key){
		Pattern p=Pattern.compile(key+"(.{1,}?:)(\\d{1,})");
		Matcher m=p.matcher(digitString);
		double digit=0.0;
		if(m.find()){
			try{
				digit=Double.parseDouble(m.group(2));
			}
			catch (Exception e) {
				return 0.0;
			}
			return digit;
		}
		return 0.0;
	}


	public static String[] getPlayCount(String updown) {
		// window.playCountVrs &&
		// window.playCountVrs({plids:{"212":{"total":1419126,"today":771}},vids:{"4668":{"total":1419132,"today":777}}})
		int index = 0;
		String temp = null;
		index = updown.indexOf("plids");
		if (index < 0)
			return null;
		index = updown.indexOf("total");
		if (index < 0)
			return null;
		temp = updown.substring(index + 7);
		index = updown.indexOf(",");
		if (index < 0)
			return null;
		temp = temp.substring(0, index);
		index = temp.indexOf("today");
		if (index < 0)
			return null;
		temp = temp.substring(index + 7);
		index = temp.indexOf("}");
		if (index < 0)
			return null;
		temp = temp.substring(0, index);
		return null;

	}

	public static int isPaid(String source) {
		Pattern p = Pattern.compile("cfix bot.*免费看");
		Matcher m = p.matcher(source);
		if (m.find())
			return 0;
		return 1;
	}

	public static String getInfoCount(String source) {
		int index = 0;
		index = source.indexOf("=");
		if (index < 0)
			return null;
		String temp = source;
		temp = temp.substring(index + 1);
		index = temp.indexOf(";");
		if (index < 0)
			return null;
		temp = temp.substring(0, index);
		return temp;
	}

	public static String[] splitNumber(String updown, String unit) {
		int index = 0;
		String temp = null;
		String temp2 = null;
		String[] items = new String[2];
		index = updown.indexOf(unit);
		if (index < 0)
			return null;
		temp = updown.substring(index + unit.length());
		index = temp.indexOf("total");
		if (index < 0)
			return null;
		temp = temp.substring(index + 7);
		temp2 = temp;
		index = temp.indexOf(",");
		if (index < 0)
			return null;
		items[0] = temp.substring(0, index);
		index = temp2.indexOf("today");
		if (index < 0)
			return null;
		temp = temp2.substring(index + 7);
		index = temp.indexOf("}");
		if (index < 0)
			return null;
		items[1] = temp.substring(0, index);
		if (items.length < 2)
			return null;
		return items;
	}
}
