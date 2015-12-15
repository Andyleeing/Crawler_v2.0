package jxHan.Crawler.Util.XML;

import java.util.HashSet;

import jxHan.Crawler.WebSite.Base.BaseURLmaker;
import jxHan.Crawler.WebSite.Base.GlobalData;

public class URLmaker {
public static void makeURL(int index,String[] args,HashSet<String> urls) {
		
		for(int i = 0;i < GlobalData.paramMaxCount;i++) {
			if(GlobalData.urlParams[index][i] == null) {
				return;
			}
			String[] argsMiddle = new String[GlobalData.paramNum];
			for(int j = 0;j < index;j++) {
					argsMiddle[j] = args[j];
			}
			argsMiddle[index] = GlobalData.urlParams[index][i];
			if(index == GlobalData.paramNum-1) {
				String url = BaseURLmaker.generateURL(argsMiddle);
				urls.add(url);
			}
			else
			{
				makeURL(index+1,argsMiddle,urls);
			}
		}
	}
}
