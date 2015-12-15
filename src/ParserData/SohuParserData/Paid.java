package ParserData.SohuParserData;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Paid {
public static String[] getPaidInfo(String source) {
	
    Pattern p=Pattern.compile("Start:info.*End:info");
    Matcher m=p.matcher(source);
    int count=0;
    String[]movieinfo=new String[7];
    int index=0;
    if (m.find()) {
    	String temp=m.group();
    	Pattern p2=Pattern.compile("(<li.{10,}?</li>)|(<p.{10,}?</p>)");
        Matcher m2=p2.matcher(temp);
        while (m2.find()) {
        count++;
    	if(count<7){
    		movieinfo[index]=parseInfo(m2.group());
    		index++;
        }else movieinfo[index]=m2.group();
	}
    }
    for(int j=0;j<movieinfo.length;j++){
    	System.out.println(movieinfo[j]);
    }
    if(movieinfo.length<7)return null;
    return movieinfo;
    
}

public static String parseInfo(String temp){
	 temp=temp.replaceAll("\\s", "");
	 Pattern p=Pattern.compile("(>)(.{1,20}?)((</a>)|(</li>))");
	 Matcher m=p.matcher(temp);
	 StringBuffer strBuff=new StringBuffer();
	 while(m.find()){
		 String tempgroup=m.group(2);
		 strBuff.append(tempgroup+"@");
	 }
	 String strReturn=strBuff.toString();
	 if(strReturn==null||strReturn.equals(""))return null;
	 strReturn=strReturn.substring(0, strReturn.length()-1);
	 return strReturn;
}
}
