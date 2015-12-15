package ParserData.YoukuParserData;



public class PeopleSplit {

	public double[] age=new double[4];
	public double[] edu=new double[5];
	public double[] occ=new double[4];
	public double[] sex=new double[2];
	
	public void clear() {
		for(int i = 0;i < 4;i++) {
			age[i] = 0;
		}
		for(int i = 0;i < 5;i++) {
			edu[i] = 0;
		}
		for(int i = 0;i < 4;i++) {
			occ[i] = 0;
		}
		for(int i = 0;i < 2;i++) {
			sex[i] = 0;
		}
	}
	public int getPlay(String str)
	{
		int play=0;
		str=str.replaceAll(",","");
		play=Integer.parseInt(str);
		return play;
	}
	
	public double getPercent(String str)
	{
		double d=0.0;
		str=str.replaceAll("%","");
		d=Double.parseDouble(str);
		d/=100;
		return d;
	}
	
	public void getAge(String str)
	{
		int num=0;
		String strAge=null;
		String strEdu=null;
		String strAcc=null;
		String strSex=null;
		String temp=null;
		if(str.contains("age"))
		{
			strAge=strFind(str,"age\":","],");
			for(int i=0;i<4;i++)
			{
				temp=strFind(strAge,"\"","\"");
				if(temp==null) break;
				age[i]=Double.parseDouble(temp);
				strAge=strAge.replaceFirst("\""+temp+"\"","");
			}	
		}
		if(str.contains("edu"))
		{
			strEdu=strFind(str,"edu\":","],");
			for(int i=0;i<5;i++)
			{
				temp=strFind(strEdu,"\"","\"");
				if(temp==null) break;
				edu[i]=Double.parseDouble(temp);
				strEdu=strEdu.replaceFirst("\""+temp+"\"","");
			}	
		}
		if(str.contains("occupation"))
		{
			strAcc=strFind(str,"occupation\":","],");
			for(int i=0;i<4;i++)
			{
				temp=strFind(strAcc,"\"","\"");
				if(temp==null) break;
				occ[i]=Double.parseDouble(temp);
				strAcc=strAcc.replaceFirst("\""+temp+"\"","");
			}	
		}
		if(str.contains("sex"))
		{
			strSex=strFind(str,"sex\":","]}");
			for(int i=0;i<2;i++)
			{
				temp=strFind(strSex,"\"","\"");
				if(temp==null) break;
				sex[i]=Double.parseDouble(temp);
				strSex=strSex.replaceFirst("\""+temp+"\"","");
			}	
		}
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
}
