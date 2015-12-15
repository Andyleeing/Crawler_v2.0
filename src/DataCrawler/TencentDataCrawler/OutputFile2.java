package DataCrawler.TencentDataCrawler;

import java.io.FileWriter;
import java.io.IOException;

import org.jsoup.nodes.Document;

import DataCrawler.CrawlerThread;
import HDFS.HDFSCrudImpl;
import URLcrawler.Tencent.ceshi;
import Utils.JDBCConnection;

public class OutputFile2 {
	public static FileWriter fx;
	public JDBCConnection jdbc;
	public OutputFile2(JDBCConnection jdbc){
		this.jdbc = jdbc;
	}
	public void tencentOutput(String s, long time) {

		// cid 即电影url后面几位
		// vid 即kid 唯一标识
		HDFSCrudImpl hdfs = new HDFSCrudImpl();
		int burl = s.indexOf(" + ");
		String strUrl = "";
		String score = "";
		String title = "";
		String cId = "";
		String allContent = "";
		int type = 1; // 判断是 腾讯影院 还是免费电影
		if (s.indexOf("film.qq.com") > 0) {
			type = 2;
		}
		if (burl > 0) { // 对应的就是info地址
			strUrl = s.substring(0, burl);
			score = s.substring(burl + 3, burl + 6);
			score = score + "   <type>m</type>";
			allContent = allContent + score;
			
			int index = strUrl.indexOf("cover", 0);
			cId = strUrl.substring(index + 8, index + 23);

			Document doc = ceshi.getdoc(strUrl);
			// 有些预告片的网址不是cover，要改成prev
			String getvid = "";
			if(doc != null){
				getvid = doc.toString();
			}
			int idPosition = getvid.indexOf("vid:", 2);
			if (idPosition == -1) {
				String totalcId = strUrl.substring(index + 6, index + 23);
				strUrl = "http://v.qq.com/prev/" + totalcId + ".html";

				doc = ceshi.getdoc(strUrl);
				if(doc != null){
					getvid = doc.toString();
					idPosition = getvid.indexOf("vid:", 2);
				} else{
					jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
					return;
				}
			}
			String vid = getvid.substring(idPosition + 5, idPosition + 16); // vid即kid
			// 出现这样的情况表示网页已经不存在
			if (vid.contains("html")) {
				// System.out.println("");
				try {
					fx = new FileWriter("src/DataCrawler/TencentLog.txt", true);
					fx.write(strUrl + " error");
				} catch (IOException e2) {
					e2.printStackTrace();
				}
				jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
			} else {
				// 写入源文件
				allContent += "<shijianchuo>" + time + "</shijianchuo>";
				allContent = allContent + "<thisisurl>" + strUrl + "</thisisurl>";
				allContent = allContent + getvid;
				String atotalWatch = "http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?id=";
				String totalWatch = atotalWatch + vid; // cid 即电影url 后面几位
				// 写入总观看人数
				try {
					doc = ceshi.getdoc(totalWatch);
					String a = doc.toString();

					allContent = allContent
							+ "-----------------总播放人数-----------------" + a;
				} catch (Exception e) {
					jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
				}
				String beginupdown = "http://sns.video.qq.com/tvideo/fcgi-bin/spvote?t=3&keyid=";
				if (type == 2) {
					beginupdown = "http://ncgi.video.qq.com/tvideo/fcgi-bin/spvote?t=3&keyid=";
				}
				String updown = beginupdown + vid;

				// 写入赞和踩人数
				try {
					doc = ceshi.getdoc(updown);
					String a = doc.toString();
					allContent = allContent
							+ "-----------------------顶与踩--------------------"
							+ a;
				} catch (Exception e) {

				}
				// 写入评论数
				// 得到评论数地址

				String getComment = "http://sns.video.qq.com/fcgi-bin/video_comment_id?op=3&cid="
						+ cId;
				if (type == 2) {
					getComment = "http://sns.video.qq.com/fcgi-bin/video_comment_id?op=3&vid="
							+ vid;
				}
				try {
					doc = ceshi.getdoc(getComment);
					String getCommentId = doc.toString();
					int cIdPosition = getCommentId.indexOf("comment_id", 0);
					// String commentId = getCommentId.substring(cIdPosition +
					// 11,
					// cIdPosition + 21);

					String commentId = getCommentId.substring(cIdPosition + 16,
							cIdPosition + 26); // vid 即kid

					// 得到评论数
					int error = commentId.indexOf("com");
					if (error < 0) {
						String aComment = "http://video.coral.qq.com/article/";
						String bComment = "/commentnum";
						String cAdress = aComment + commentId + bComment;

						doc = ceshi.getdoc(cAdress);
						String a = doc.toString();
						allContent = allContent
								+ "-----------------------评论数----------------------"
								+ a;

					}
				} catch (Exception e) {

				}

				String aguessWYL = "http://like.video.qq.com/fcgi-bin/recommand?tablist=1%3B2%3B3%3B4%3B5&playright=2&size=24&uin=0&cid=";
				String guessWYL = aguessWYL + cId;
				try {
					doc = ceshi.getdoc(guessWYL);
					String a = doc.toString();
					allContent = allContent
							+ "-------------------猜你喜欢-----------------" + a;
				} catch (Exception e) {

				}
				// delete space
				allContent = allContent.replaceAll("\n", "");
				allContent = allContent.replaceAll("&middot", ".");
				// save to local big file
			CrawlerThread.saveData("tencent " + cId, allContent);
			}
		} else { // else里面写的是分集的情况
			burl = s.indexOf("  ");
			strUrl = s.substring(0, burl);
			int nameEnd = s.indexOf(" * ");
			title = s.substring(burl + 2, nameEnd) + "   <type>m</type>";
			int index = strUrl.indexOf("cover", 0);
			if(index<0){
				index = strUrl.indexOf("/prev");
			}
			cId = strUrl.substring(index + 8, index + 23);
			String vid = s.substring(index + 24, index + 35);
			if(vid.contains("html")){
				return;  //20150605   这种情况对应的应该是info页，已经抓过
			}
			// 写入每一集名称
			allContent = allContent + title;

			// 写入每一集的外键

			allContent = allContent + "cId:" + cId + "</cId>";
			allContent = allContent + "<thisisurl>" + strUrl + "</thisisurl>";
			allContent += "<shijianchuo>" +time + "</shijianchuo>";

			// 写入总观看人数
			String atotalWatch = "http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?id=";
			String totalWatch = atotalWatch + vid; // vid 每一集唯一标识

			try {
				Document doc = ceshi.getdoc(totalWatch);
				String a = doc.toString();

				allContent = allContent
						+ "-----------------------总播放人数----------------------"
						+ a;
			} catch (Exception e) {
				jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
			}

			// 写入赞和踩人数
			try {
				String beginupdown = "http://sns.video.qq.com/tvideo/fcgi-bin/spvote?t=3&keyid=";
				if (type == 2) {
					beginupdown = "http://ncgi.video.qq.com/tvideo/fcgi-bin/spvote?t=3&keyid=";
				}
				String updown = beginupdown + vid;

				Document doc = ceshi.getdoc(updown);
				String a = doc.toString();
				allContent = allContent
						+ "----------------------顶与踩---------------------" + a;
			} catch (Exception e) {

			}
			// 写入评论数
			// 得到评论数地址
			try {
				String getComment = "http://sns.video.qq.com/fcgi-bin/video_comment_id?op=3&vid="
						+ vid;

				Document doc = ceshi.getdoc(getComment);

				String getCommentId = doc.toString();
				int cIdPosition = getCommentId.indexOf("comment_id", 0);
				// String commentId = getCommentId.substring(cIdPosition + 11,
				// cIdPosition + 21); // vid 即kid
				String commentId = getCommentId.substring(cIdPosition + 16,
						cIdPosition + 26); // vid 即kid

				int error = commentId.indexOf("com");
				if (error < 0) {

					// 得到评论数

					String aComment = "http://video.coral.qq.com/article/";
					String bComment = "/commentnum";
					String cAdress = aComment + commentId + bComment;

					doc = ceshi.getdoc(cAdress);
					String a = doc.toString();

					allContent = allContent
							+ "-----------------------评论数--------------------"
							+ a;
				}
			} catch (Exception e) {

			}
			// delete space
			allContent = allContent.replaceAll("\n", "");
			allContent = allContent.replaceAll("&middot", ".");
			// save to local big file
			CrawlerThread.saveData("tencent " + cId + "+" + vid, allContent);
		}
	}
}
