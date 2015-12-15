package ParserData.SohuParserData;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
public static void main(String args[]) {
	
	String sumplaycount = "3.70000".replaceAll("\\.", "").replaceAll("0[1]","");
	System.out.print(sumplaycount);
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
}
