package DataCrawler.DataCrawler_56;

import Utils.JDBCConnection;

public class CrawlerContent
{
	public Share share = new Share();
	public JDBCConnection jdbc;
	public CrawlerContent(JDBCConnection jdbc) {
		this.jdbc=jdbc;
	}
	public int main56Content(String URL,long time)
	{
		String item = null;
		String id = null;
		String infoId = null;
		String url = null;
		String category = null;
		String kind = null;
		String showtype=null;
		String[] str = null;
		int flag ;//count
		
		//System.out.println("url="+URL);
		item=URL.substring(3);//去掉56和空格；
		//System.out.println("item="+item);
		if(item!=null&&!item.equals(""))
		{
			str=item.split("@");
			id=str[0];
			url=str[1];
			category=str[2];
			kind=str[3];
//			System.out.println("id="+id);
//			System.out.println("url="+url);
//			System.out.println("category="+category);
//			System.out.println("kind="+kind);
			if(url.contains("v_.html")) return 0;
//			{
//				jdbc.log("师玉龙", "56+"+id, 1, "56", url, "invalid url", 1);
//				
//			}
			
			if(kind.equals("Info")) 
			{
				showtype=str[4];
				//System.out.println("showtype="+showtype);
				flag=Integer.parseInt(str[5]);
				//System.out.println("flag="+flag);
				return share.storeListInfo(id,url,category,kind,showtype,time,flag,jdbc);
			}
			else 
			{
				infoId=str[4];
				showtype=str[5];
				flag=Integer.parseInt(str[6]);
				return share.storeListPlay(id,url,category,kind,infoId,showtype,time,flag,jdbc);
			}
		}
		return 1;
	}

}


