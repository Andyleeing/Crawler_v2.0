package DataCrawler.TencentDataCrawler;

import java.io.FileWriter;
import java.io.IOException;

import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import DataCrawler.CrawlerThread;
import URLcrawler.Tencent.ceshi;
import Utils.JDBCConnection;
public class OutputFileZongYi2 {
	public static FileWriter fx;
	public static FileWriter fw;
	public JDBCConnection jdbc;
	public OutputFileZongYi2(JDBCConnection jdbc){
		this.jdbc=  jdbc;
	}
	public static void main(String[] args)  {
		
	}
	public void tencentOutput(String s, long time) {	
		int burl = s.indexOf(" + ");
		if (s.contains(" + ")) { // 对应的就是info地址
			crawlerInfo(s,time);
		} else { // else里面写的是分集的情况
			crawlerPlay(s,time);
		}
	}

	public void crawlerInfo(String s,long time) {
		/*
		 * 保存info页数据的代码 info页存取格式： http://v.qq.com/detail/1/1068.html + 0.0 *
		 * type:zongyi 作者：石嘉帆 每个info页保存格式：
		 * 0.0  <type>z</type><thisisurl>strurl</thisisurl>content
		 */
		StringBuffer allContent = new StringBuffer();
		int burl = s.indexOf(" + ");
		String strurl = null;
		try {
			strurl = s.substring(0, burl);
		} catch (Exception e) {
		//	e.printStackTrace();
		//	jdbc.log("石嘉帆", cId, 1, "tx", s, "无连接", 1);
			 return;
		}
		int aInfoid = strurl.indexOf("detail");
		int bInfoid = strurl.indexOf(".html");
		String infoid = null;
		try {
			infoid = strurl.substring(aInfoid + 7, bInfoid);
		} catch (Exception e) {
	//		e.printStackTrace();
			return;
		}

		Document doc = ceshi.getdoc(strurl);
		allContent.append("0.0" + "   <type>z</type>"); // 加入类型，评分是为了和影视动漫格式对应，方便解析
		allContent.append("<thisisurl>" + strurl + "</thisisurl>");// 加入url
		allContent.append("<infoid>" + infoid + "</infoid>");
		allContent.append("<shijianchuo>" + time + "</shijianchuo>");
		if (doc == null) {
			try {
				fx = new FileWriter("src/DataCrawler/TencentLog.txt",true);
				fx.write(strurl+ "\n");
			} catch (IOException e) {
		//		e.printStackTrace();
			}
			return;
		}
		allContent.append(doc.toString());
		/*
		 * info页也有猜你喜欢
		 * info页的猜你喜欢用的是id号后面几位
		 */
		allContent.append("--------------猜你喜欢开始--------------");
		String guess= "http://like.video.qq.com/fcgi-bin/rmd_open?dtype=1&id=1509&size=1000";
		String aguess = "http://like.video.qq.com/fcgi-bin/rmd_open?dtype=1&id=";
		String bguess = "&size=1000";
		//  gid = infoid 的后面几位
		String gid  = infoid.substring(2);
		String guessUrl = aguess + gid + bguess;
		doc = ceshi.getdoc(guessUrl);
		if(doc != null){
			allContent.append(doc.toString());
		}		
		String save = allContent.toString();
		save = save.replaceAll("\n", "");//去除空格
		save = save.replaceAll("&middot", ".");
		/*
		 * 为了解析方便，存入的id最好有个标识，标识 视频的类型（影视动漫综艺） 这里infoid和别的id号都不一样，所以暂时可以不用管
		 * 影视，动漫这三个就是因为没有标识，所以解析代码加到了一起
		 */
		CrawlerThread.saveData("tencent " + infoid, save);
		allContent = null;
		save = null;
	}

	public void crawlerPlay(String s,long time) {
		/*
		 * 保存play页数据的代码 play页url格式： http://v.qq.com/cover/e/eladr1pi73617wz.html
		 * 成语学《易》更容易 @ 2012-12-30 * type:zongyi 作者：石嘉帆 每个info页保存格式: name
		 * <type>z</type>
		 */
        StringBuffer allContent = new StringBuffer();
		// 截取url
		int burl = s.indexOf("  ");
		String strUrl = s.substring(0, burl);
		// 截取名称
		int nameEnd = s.indexOf(" @ ");
		String title = s.substring(burl + 2, nameEnd) + "   <type>z</type>";
		// 截取第几期
		int dateEnd = s.indexOf(" * ");
		String date = s.substring(nameEnd + 3, dateEnd);
		// 截取id号
		int index = strUrl.indexOf("cover", 0);
		String cid = strUrl.substring(index + 8, index + 23);

		allContent.append(title); // 加入名称
		allContent.append("<datee>" + date + "</datee>");// 加入日期
		allContent.append("<thisisurl>" + strUrl + "</thisisurl>"); // 加入url
		allContent.append("<cid>" + cid + "</cid>"); // 加入cid
		allContent.append("<shijianchuo>" + time + "</shijianchuo>");

		// 保存原网页
		Document doc = ceshi.getdoc(strUrl);
		if(doc == null){
			return;
		}
		Elements link = doc.getElementsByAttributeValue("_hot", "cover3.pastcovers.jumpdetail");
		String infoUrl  = link.attr("abs:href");
		int aInfoid = infoUrl.indexOf("detail");
		int bInfoid = infoUrl.indexOf(".html");
		String infoid = "";
		if(aInfoid>0 && bInfoid>aInfoid){
			infoid = infoUrl.substring(aInfoid + 7, bInfoid);
		} 
		allContent.append("<fatherId>" + infoid + "</fatherId>");
		allContent.append("<infoUrl>" + infoUrl + "</infoUrl>");
		
		String page = "";
		try {
			page = doc.toString();
			
		} catch (Exception e2) {
			jdbc.log("石嘉帆", infoid + "+tx", 1, "tx", s, "无连接", 1);
			e2.printStackTrace();
		}
		allContent.append(page);
		// 保存总的播放数
		String atotalWatch = "http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?id=";
		String totalWatch = atotalWatch + cid;
		doc = ceshi.getdoc(totalWatch);
		page = "";
		try {
			page = doc.toString();
		} catch (Exception e1) {
			jdbc.log("石嘉帆", infoid + "+tx", 1, "tx", s, "无连接", 1);
	//		e1.printStackTrace();
		}
		allContent.append("-----------------总播放人数-----------------");
		allContent.append(page);
		// 保存评论数
		String getComment = "http://sns.video.qq.com/fcgi-bin/video_comment_id?op=3&cid="
				+ cid;
		try {
			doc = ceshi.getdoc(getComment);
			String getCommentId = doc.toString();
			int cIdPosition = getCommentId.indexOf("comment_id", 0);
			String commentId = getCommentId.substring(cIdPosition + 16,
					cIdPosition + 26); // vid 即kid

			// 得到评论数
			String aComment = "http://video.coral.qq.com/article/";
			String bComment = "/commentnum";
			String cAdress = aComment + commentId + bComment;

			doc = ceshi.getdoc(cAdress);
			page = doc.toString();
			allContent
					.append("-----------------------评论数----------------------");
			allContent.append(page);

		} catch (Exception e1) {
	//		e1.printStackTrace();
		}
		//保存猜你喜欢
		allContent.append("-------------------猜你喜欢开始-----------------");
		String aguess = "http://like.video.qq.com/fcgi-bin/recommand?tablist=1%3B2%3B3%3B4%3B5&playright=2&size=24&uin=0&cid=";
		String guessUrl = aguess + cid ;
		doc = ceshi.getdoc(guessUrl);
		if(doc != null){
			allContent.append(doc.toString());
		}
		
		String save = allContent.toString();
		// delete the space
		save = save.replaceAll("\n", "");
		save = save.replaceAll("&middot", ".");
		
		
		CrawlerThread.saveData("tencent " + cid + " type:zongyi", save);
		allContent = null;
		save = null;
	}
}
