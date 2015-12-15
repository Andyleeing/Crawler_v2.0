package DataCrawler.SohuDataCrawler;

import java.net.UnknownHostException;

import javax.ws.rs.core.Variant.VariantListBuilder;

import org.apache.commons.math.stat.descriptive.StatisticalSummaryValues;

public class CrawlDynamic {
	
      public JHtmlUpdateCheck  	JU= new JHtmlUpdateCheck();
	
	public  String[] getPlay(String category,String vid,String playlistid) {
		String[]items=new String[5];
		int index=0;
		String currentV=null;
		String currentU=null;
		String[]urltypes={Variables.UPDOWN,Variables.PCOUNT,Variables.COMMENT,Variables.GUESSLIKE,Variables.PEOPELWATCH};
		int circleLength=4;
		int type=0;
		if(category.indexOf(Variables.DIANYING)>=0){
			circleLength=5;
			type=Variables.MOVIEUPDOWN;
		}
		else if(category.indexOf(Variables.DONGMAN)>=0)type=Variables.DMUPUPDOWN;
		else if(category.indexOf(Variables.YINGSHI)>=0)type=Variables.YSUPUPDOWN;
		else if(category.indexOf(Variables.ZONGYI)>=0)type=Variables.ZYUPDOWN;
		for(int j=0;j<circleLength;j++){
			currentU=getURL(urltypes[j],type, playlistid, vid);
			currentV=JU.visitURL(currentU);
			if(currentV==null||currentV.equals(""))return null;
			items[index]=currentV;
			index++;
		}
		if(items==null||items.length<circleLength)return null;
		return items;
	}
	public  String[] getPaidPlay(String category,String vid,String playlistid) {
		String[]items=new String[5];
		int index=0;
		String currentV=null;
		String currentU=null;
		String[]urltypes={Variables.UPDOWN,Variables.COMMENT};
		int circleLength=2;
		int type=Variables.MOVIEUPDOWN;
		for(int j=0;j<circleLength;j++){
			currentU=getURL(urltypes[j],type, playlistid, vid);
			currentV=JU.visitURL(currentU);
			if(currentV==null||currentV.equals(""))return null;
			items[index]=currentV;
			index++;
		}
		if(items==null||items.length<circleLength)return null;
		return items;
	}
	public String[] getInfo(String category,String playlistid)  {
		int length=2;
		String[]items=new String[2];
		String currentU=null;
		currentU=getURL(Variables.MARK, 0, playlistid, null);
		items[0]=JU.visitURL(currentU);
		if(category.indexOf(Variables.DIANYING)>=0){
			length=1;
		}else{
			currentU=getURL(Variables.ZHISHU, 0, playlistid, null);
			items[1]=JU.visitURL(currentU);
		}
		if(items==null||items.length<length)return null;
		return items;
		
	}
	public String getInfoCount(String playlistid) {
		String urlString=getURL(Variables.INFOCOUNT, 0, playlistid, null);
		String source=JU.visitURL(urlString);
		return source;
	}
	public  String getURL(String urlname,int type,String playlistid,String vid){
		String url = null;
		if(urlname.equals(Variables.UPDOWN)){//--pass
			url="http://score.my.tv.sohu.com/digg/get.do?vid="
					+ vid + "&type="+type;
		}else if(urlname.equals(Variables.PCOUNT)){//--pass
			url = "http://count.vrs.sohu.com/count/queryext.action?vids="+vid+"&plids="+playlistid+"&callback=playCountVrs";
		}else if(urlname.equals(Variables.COMMENT)){//--pass
			url="http://access.tv.sohu.com/reply/list/1000_"
				+ playlistid + "_" + vid + "_0_1.js";
		}else if(urlname.equals(Variables.GUESSLIKE)){//--pass
			url="http://search.vrs.sohu.com/p?&vid="
				+ vid+ "&pageNum=1&pageSize=10&source=20&cateid=&cate=&var=similar&test=2&ab=0";
		}else if(urlname.equals(Variables.PEOPELWATCH)){//--pass ����Ҷ��ڿ��� ���Ĳ�Ƭ���޹� special for movie
			url = "http://pl.hd.sohu.com/recommend_frag?ids=20092852&types=2&nos=14&callback=tuijianCall";
		}else if(urlname.equals(Variables.MARK)){
			url = "http://vote.biz.itc.cn/count_v77_t1_i" + playlistid+ "_b_c.json";//--pass
		}else if(urlname.equals(Variables.ZHISHU)){
			url = "http://index.tv.sohu.com/index/switch-app/"+ playlistid + ".jsonp";
		}else if(urlname.equals(Variables.INFOCOUNT)){
			url="http://count.vrs.sohu.com/count/query_album.action?albumId="+playlistid+"&type=2";
		}
		return url;
	}

}
