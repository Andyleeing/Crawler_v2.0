package ParserData.YoukuParserData;



public class Province {

	public int pro[][] = new int[36][2];
	public void clear() 
	{
		for(int i = 0;i < 36;i++) {
			pro[i][1] = -1;
			pro[i][0] = -1;
		}
	}
	
	public void province(String str)
	{
		//System.out.println(str);
		String temp;
		String prov;
		if(str.contains("city"))
		{
			if(str.contains("440300"))
			{
				temp=strFind(str, "440300:", "\"");
				pro[0][0]=440300;
				pro[0][1]=Integer.parseInt(temp);
			}
			if(str.contains("440100"))
			{
				temp=strFind(str, "440100:", "\"");
				pro[1][0]=440100;
				pro[1][1]=Integer.parseInt(temp);
			}
		}
		if(str.contains("provinceMap"))
		{
			prov=strFind(str, "provinceMap\":[", "]");
			String[] splits = prov.split(",");
			for(int i = 0;i < splits.length;i++) {
				int index = i + 2;
				String[] subSplits = splits[i].split(":");
				if(subSplits.length < 2)
					continue;
				int number = -1;
				int count = -1;
				if(subSplits[0].length() > 1) {
					String numStr = subSplits[0].substring(1);
					number = Integer.parseInt(numStr);
				}
				if(number < 0)
					continue;
				if(subSplits[1].length() > 1) {
					int Mindex = subSplits[1].indexOf("\"");
					if(Mindex > 0)
						subSplits[1] = subSplits[1].substring(0,Mindex);
					count = Integer.parseInt(subSplits[1]);
				}
				if(count < 0)
					continue;
				pro[index][0] = number;
				pro[index][1] = count;
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
