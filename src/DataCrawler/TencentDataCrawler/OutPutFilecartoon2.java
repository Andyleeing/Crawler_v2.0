package DataCrawler.TencentDataCrawler;

import org.jsoup.nodes.Document;

import DataCrawler.CrawlerThread;
import HDFS.HDFSCrudImpl;
import URLcrawler.Tencent.ceshi;
import Utils.JDBCConnection;

public class OutPutFilecartoon2 {
	public JDBCConnection jdbc;
	public OutPutFilecartoon2(JDBCConnection jdbc){
		this.jdbc = jdbc;
	}
	
	public void tencentOutput(String s, long time) {
		// cid 即电影url后面几位
		// vid 即kid 唯一标识
		// File file1 = new File("D:\\test\\url\\cartoonUrl.txt");// Text文件
		// BufferedReader br = new BufferedReader(new FileReader(file1));//
		// 构造一个BufferedReader类来读取文件

		// while((s = br.readLine())!=null){//使用readLine方法，一次读一行
		HDFSCrudImpl hdfs = new HDFSCrudImpl();
		int burl = s.indexOf(" + ");
		// 如果有3个空格，表示首页
		String allContent = "";
		String strUrl = "";
		String score = "";
		String cId = "";
		String title = "";
		if (burl > 0) {
			try {
				score = s.substring(burl + 3, burl + 6); // 得到评分
				score = score + "   <type>c</type>";
				allContent = allContent + score;
				allContent = allContent + "<shijianchuo>" + time
						+ "</shijianchuo>";


				int index = s.indexOf("cover", 0);
				//下面这行曾经出过错
				strUrl = "http://v.qq.com/" + s.substring(index, burl); // 得到url  可能会报错！！！
				cId = strUrl.substring(index + 8, index + 23);

				Document doc = ceshi.getdoc(strUrl);
				allContent = allContent + "<thisisurl>" + strUrl
						+ "</thisisurl>";
				// 写入源文件
				try {
					allContent = allContent + doc.toString();
				} catch (Exception e) {
					jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
				}
				// 写入总观看人数
				try {
					String atotalWatch = "http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?id=";
					String totalWatch = atotalWatch + cId; // cid 即电影url 后面几位

					doc = ceshi.getdoc(totalWatch);
					// "
					allContent = allContent
							+ "---------------总播放人数--------------------"
							+ doc.toString();
				} catch (Exception e) {
					jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
				}
				// 猜你喜欢
				try {
					String aguessWYL = "http://like.video.qq.com/fcgi-bin/recommand?tablist=1%3B2%3B3%3B4%3B5&playright=2&size=24&uin=0&cid=";
					String guessWYL = aguessWYL + cId;

					doc = ceshi.getdoc(guessWYL);

					allContent = allContent
							+ "----------------------猜你喜欢----------------------"
							+ doc.toString();
				} catch (Exception e) {

				}
				// delete space
				allContent = allContent.replaceAll("\n", "");
				allContent = allContent.replaceAll("&middot", ".");
				// save to local big file
				CrawlerThread.saveData("tencent " + cId , allContent);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// 对应的每一集
		else {
			burl = s.indexOf("  ");
			strUrl = s.substring(0, burl);
			int nameEnd = s.indexOf(" * ");
			title = s.substring(burl + 2, nameEnd) + "   <type>c</type>";
			allContent = allContent + title;

			int index = strUrl.indexOf("cover", 0);
			cId = strUrl.substring(index + 8, index + 23);
			String vid = s.substring(index + 24, index + 35);

			// 写入每一集的外键

			allContent = allContent + "cId:" + cId + "</cId>";
			allContent = allContent + "<thisisurl>" + strUrl + "</thisisurl>";
			allContent += "<shijianchuo>" + time + "</shijianchuo>";
			// 写入总观看人数
			try {
				String atotalWatch = "http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?id=";
				String totalWatch = atotalWatch + vid; // vid 每一集唯一标识

				Document doc = ceshi.getdoc(totalWatch);

				allContent = allContent
						+ "-------------------总播放人数---------------------"
						+ doc.toString();
			} catch (Exception e) {
				jdbc.log("石嘉帆", cId + "+tx", 1, "tx", s, "无连接", 1);
			}
			// 写入赞和踩人数
			try {
				String beginupdown = "http://sns.video.qq.com/tvideo/fcgi-bin/spvote?t=3&keyid=";
				String updown = beginupdown + vid;

				Document doc = null;
				doc = ceshi.getdoc(updown);

				allContent = allContent
						+ "---------------------顶与踩-----------------------"
						+ doc.toString();
			} catch (Exception e) {

			}
			// 写入评论数
			// 得到评论数地址
			try {
				String getComment = "http://sns.video.qq.com/fcgi-bin/video_comment_id?op=3&vid="
						+ vid;
				Document doc = null;
				doc = ceshi.getdoc(getComment);

				String getCommentId = doc.toString();
				int cIdPosition = getCommentId.indexOf("comment_id", 0);
				// String commentId = getCommentId.substring(cIdPosition + 11,
				// cIdPosition + 21);

				String commentId = getCommentId.substring(cIdPosition + 16,
						cIdPosition + 26); // vid 即kid

				int error = commentId.indexOf("com");
				if (error < 0) {

					// 得到评论数

					String aComment = "http://video.coral.qq.com/article/";
					String bComment = "/commentnum";
					String cAdress = aComment + commentId + bComment;
					doc = null;
					doc = ceshi.getdoc(cAdress);

					allContent = allContent
							+ "------------------------评论数-----------------------"
							+ doc.toString();
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
