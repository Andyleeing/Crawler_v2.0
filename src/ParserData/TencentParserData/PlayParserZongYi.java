package ParserData.TencentParserData;

import hbase.HBaseCRUD;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import Utils.JDBCConnection;
import Utils.TextValue;

public class PlayParserZongYi {
	
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	public PlayParserZongYi(HBaseCRUD hbase,JDBCConnection jdbconn){
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}

	public static void analyse(String rowkey, String page) {
		/*
		 * rowkey是每个视频内容的标记，page就是保存的视频的所有内容 传进来的url分为info和play的，要进行判断，执行不同的解析代码
		 */
//		if (!rowkey.contains("type")) {
//			new PlayParserZongYi().analyseInfo(page);
//		} else {
//			new PlayParserZongYi().analysePlay(page);
//		}
	}

	public  void analyseInfo(String page) {
		/*
		 * 对info页的解析 主要函数有两个，analyse适合解析出来单一元素，比如年份，名称
		 * analyseAll适合解析多个结果，比如演员，片段，猜你喜欢等。 开头：0.0
		 * <type>z</type><thisisurl>strurl</thisisurl>content
		 * <infoid><shijianchuo>才你喜欢
		 */
		String title = null; // 综艺名称
		String picUrl = null; // 图片url
		String host = null; // 主持人
		String channel = null; // 电视台
		String label = null; // 标签
		String info = null; // 简介
		String type = "zongyi";
		String url = null;
		// String timeStamp = null;
		String time = "";
		String infoid = null;
		String guess = null;

		infoid = analyse(page, "<infoid>", "</infoid>", 8);
		time = analyse(page, "<shijianchuo>", "</shijianchuo>", 13);
		url = analyse(page, "<thisisurl>", "</thisisurl>", 11);
		title = analyse(page, "vtitle : '", "',", 10);
		picUrl = analyse(page, "vpic : '", "',", 8);
		host = analyseAll(page, "主持人：", "电视台：", "title=\"", "\" itemprop=", 7);
		channel = analyseAll(page, "电视台：", "简介：", "itemprop=\"name\">",
				"</span></a>", 16);
		label = analyseAll(page, "标签：", "主持人：", "btn_inner\">", "</span></a>",
				11);
		info = analyse(page, "description\">", "</span>", 13);
		/*
		 * 猜你喜欢部分
		 */
		int a = page.indexOf("-----猜你喜欢开始------");
		if (a > 0) {
			String cainixihuanPage = page.substring(a);
			guess = playGuess(cainixihuanPage);
		} else {
			guess = "";
		}
		/*
		 * 将info页数据存入表1 movieinfo 暂时没有保存rul 也是"B“组
		 */

		String key1 = infoid + "+" + "tx";
		String key2 = infoid + "+" + "tx" + "+" + time;
		// String url = "http://www.letv.com/comic/" + rowkey + ".html";
		String[] rows = new String[] { key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1 };
		String[] colfams = new String[] { "R", "R", "B", "B", "B", "B", "B",
				"B", "B", "B" };
		String[] quals = new String[] { "inforowkey", "website", "name",
				"pictureURL", "area", "type", "director", "category",
				"summarize", "url" };
		String[] values = new String[] { infoid, "tx", title, picUrl, channel,
				label, host, type, info, url };
		try {
			hbase.putRows("movieinfo", rows, colfams, quals, values);
			hbase.putRows("movieinfobak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		 * 将info页动态信息存入表2
		 */

		rows = new String[] { key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "C", "C", "C" };
		quals = new String[] { "inforowkey", "website", "timestamp", "name",
				"tengxunrelated", "free" };
		values = new String[] { infoid, "tx", time, title, guess, "1" };
		try {
			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("iforowkey:" + infoid);
		// System.out.println("name:" + title);
		// System.out.println("pictureURL:" + picUrl);
		// System.out.println("area:" + channel);
		System.out.println("type:" + label);
		// System.out.println("director:" + host);
		// System.out.println("category:" + type);
		// System.out.println("summarize:" + info);
		// System.out.println("url:" + url);
		// System.out.println("timestamp:" + time);
		System.out.println("tengxunrelated:" + guess);

		// 存入mysql
		ArrayList<TextValue> values2 = new ArrayList<TextValue>();

		TextValue crawltimetv = new TextValue();
		crawltimetv.text = "crawltime";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = new Date();
		crawltimetv.value = sdf.format(d);
		values2.add(crawltimetv);

		TextValue yeartv = new TextValue();
		yeartv.text = "year";
		yeartv.value = -1;
		values2.add(yeartv);

		if (info != "") {
			TextValue summarizetv = new TextValue();
			summarizetv.text = "summarize";
			info = cleanString(info);
			if (info.length() > 999) {
				info = info.substring(0, 999);
			}
			summarizetv.value = info;
			values2.add(summarizetv);
		}

		TextValue timetv = new TextValue();
		timetv.text = "time";
		timetv.value = -1;
		values2.add(timetv);

		TextValue ytimetv = new TextValue();
		ytimetv.text = "ytime";
		ytimetv.value = -1;
		values2.add(ytimetv);

		String directorString = host;
		if (!directorString.equals("")) {
			TextValue directortv = new TextValue();
			directortv.text = "director1";
			directorString = cleanString(directorString);
			int index = directorString.indexOf("@");
			if (index >= 0) {
				try {
					directorString = directorString.substring(1);
					index = directorString.indexOf("@");
					if (index > 0) {
						directorString = directorString.substring(0, index);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (directorString.length() > 20) {
					directorString = "";
				}
			}
			directortv.value = directorString;
			values2.add(directortv);
		}

		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = infoid + "+tx";
		values2.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		title = cleanString(title);
		nametv.value = title;
		values2.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "tx";
		values2.add(namewebsite);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		categorytv.value = "zongyi";
		values2.add(categorytv);

		// movietype:动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线
		// cartoontype:经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载
		// zongyitype：综合访谈选秀搞笑情感脱口秀职场游戏<歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场
		String totalType = "综合访谈选秀搞笑情感脱口秀职场游戏<歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场";
		int k = 0;
		String[] typeSplits = null;
		String typeString = label.toString();
		if (type != null) {
			if (typeString.contains("@")) {
				try {
					typeString = typeString.substring(1);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			typeSplits = typeString.split("@");
		}
		for (int i = 0; i < typeSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (k + 1);
			if (totalType.contains(typeSplits[i])) {
				if (typeSplits[i].equals("") || typeSplits[i].equals(" ")) {
					continue;
				}
				typetv.value = typeSplits[i];
				k++;
				values2.add(typetv);
				if (k == 3)
					break;
			}
		}

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		duratv.value = -1;
		values2.add(duratv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = "-1";
		values2.add(pricetv);

		jdbconn.insert(values2, "movieinfo");
		// / 存入mysql 结束

		// / 存入mysql moviedynamic开始
		ArrayList<TextValue> values3 = new ArrayList<TextValue>();
		values3.add(rowkeytv);

		TextValue uptv = new TextValue();
		uptv.text = "up";
		int up = -1;
		uptv.value = up;
		values3.add(uptv);

		TextValue downtv = new TextValue();
		downtv.text = "down";
		int down = -1;
		downtv.value = down;
		values3.add(downtv);

		TextValue commentCounttv = new TextValue();
		commentCounttv.text = "comment";
		int commentCount = -1;
		commentCounttv.value = commentCount;
		values3.add(commentCounttv);

		TextValue todayCounttv = new TextValue();
		todayCounttv.text = "todayplaycount";
		int todayCount = -1;
		todayCounttv.value = todayCount;
		values3.add(todayCounttv);

		TextValue man = new TextValue();
		man.text = "man";
		man.value = -1;
		values3.add(man);

		TextValue freetv = new TextValue();
		freetv.text = "free";
		freetv.value = 1;
		values3.add(freetv);

		TextValue women = new TextValue();
		women.text = "women";
		women.value = -1;
		values3.add(women);

		TextValue scoretv = new TextValue();
		scoretv.text = "score";
		scoretv.value = -1;
		values3.add(scoretv);

		values3.add(categorytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = -1;
		values3.add(sumPlayCounttv);

		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// 信息页 2:y 指数页
		values3.add(flagtv);

		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = time.substring(0,10);
		values3.add(timestamptv);

		// TextValue movienametv = new TextValue();
		// movienametv.text = "movieName";
		// movienametv.value = nameString;
		// values.add(movienametv);
		TextValue reference = new TextValue();
		reference.text = "reference";
		reference.value = -1;
		values3.add(reference);

		values3.add(namewebsite);
		String sd = sdf.format(new Date(Long.parseLong(time)));
		jdbconn.insert(values3, "moviedynamic" + sd);
		// 存入 mysql moviedynamic 结束
	}

	private String analyseAll(String page, String kaitou, String jiewei,
			String begin, String end, int num) {
		StringBuffer sb = new StringBuffer();
		String answer = "";
		// 先将目标所在的位置截取下来
		int a = page.indexOf(kaitou);
		int b = page.indexOf(jiewei);
		if (a >= b || a < 0) {
			return answer;
		}
		String answerPage = page.substring(a, b);
		int finish = 0;
		while (true) {
			int start = answerPage.indexOf(begin, finish);
			if (start < 0) {
				sb.append("");
				break;
			} else {
				finish = answerPage.indexOf(end, start + num);
				if (start > finish || finish < 0) {
					break;
				}
				String str = answerPage.substring(start + num, finish);
				sb.append("@" + str);
			}
		}

		answer = sb.toString();
		return answer;
	}

	public void analysePlay(String page) {
		/*
		 * 对play页的解析 title:是指视频本身的名称 name：是指所属视频
		 * 保存网页的时候，除了愿望页，自己加的字段：<thisisurl><datee><cid><shijianchuo><fatherId>
		 * <infoUrl> 播放数，评论数，才你喜欢
		 */
		String title = null; // 视频名称
		// String name = null;// 综艺名称
		// String secTitle = null;// 视频副标题
		// String date = null; // 第几期 2009-08-05
		// String type = "zongyi";//
		String url = null; //
		// String contentLocation = null;// 地区
		// String inLanguage = null;// 语言
		// String duration = null;// 时长
		// String updatePeriod = null;// 综艺更新日期

		// String brief = null;// 简介
		// String pic = null;// 图片url
		String cid = null;// cid
		// String fragment = null;// 精彩片段
		String fatherid = null; // info页 id
		// String infoUrl = null;
		String playCount = null; // 播放num
		String commentNum = null; // 评论书
		String guess = null;// 才你喜欢
		String time = "";

		time = analyse(page, "<shijianchuo>", "</shijianchuo>", 13);

		title = playTitle(page);
		url = playUrl(page);
		/*
		 * 这些代码都是可用的，只是这些数据好像不需要解析
		 */
		// secTitle = ppzy.playSecTitle(page);
		// date = ppzy.playDate(page);
		cid = playCid(page);
		// contentLocation = ppzy.playContentLocation(page);
		// inLanguage = ppzy.playInLanguage(page);
		// duration = ppzy.playDuration(page);
		// updatePeriod = ppzy.playUpdatePeriod(page);
		// name = ppzy.playName(page);
		// brief = playBrief(page);
		// pic = playPic(page);
		// fragment = ppzy.playFragment(page);

		fatherid = analyse(page, "<fatherId>", "</fatherId>", 10);
		// infoUrl =analyse(page,"<infoUrl>","</fatherUrl>",9);
		playCount = analyse(page, "<all>     ", "    </all>", 10);
		commentNum = analyse(page, ";commentnum", "&quot;}", 24);
		if(commentNum.equals("")){
			jdbconn.log("石嘉帆", fatherid + "+tx", 1, "tx", url, "无播放量", 2);
		}

		int a = page.indexOf("-----猜你喜欢开始-----");
		if (a > 0) {
			String cainixihuanPage = page.substring(a);
			guess = playGuess(cainixihuanPage);
		} else {
			guess = "";
		}
		/*
		 * play to hbase
		 */
		String key3 = fatherid + "+" + cid + "+" + "tx";
		String key4 = fatherid + "+" + cid + "+" + "tx" + "+" + time;
		// url = "http://www.letv.com/ptv/vplay/" + key + ".html";

		String[] rows = new String[] { key3, key3, key3, key3, key3 };
		String[] colfams = new String[] { "R", "R", "R", "B", "B" };
		String[] quals = new String[] { "inforowkey", "playrowkey", "website",
				"name", "url" };
		String[] values = new String[] { fatherid, cid, "tx", title, url };
		try {
			hbase.putRows("videoinfo", rows, colfams, quals, values);
			hbase.putRows("videoinfobak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		rows = new String[] { key4, key4, key4, key4, key4, key4, key4 };
		colfams = new String[] { "R", "R", "R", "R", "C", "C", "C" };
		quals = new String[] { "inforowkey", "playrowkey", "website",
				"timestamp", "related", "sumplaycount", "comment" };
		values = new String[] { fatherid, cid, "tx", time, guess, playCount,
				commentNum };
		try {
			hbase.putRows("videodynamic", rows, colfams, quals, values);
			// hbase.putRows("videodynamicbak", rows, colfams, quals, values);
			hbase.putRows("videodynamicbaktx2", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.out.println("综艺play");
		// System.out.println("fid :"+fatherid);
		// System.out.println(cid);
		// System.out.println("title :"+title);
		// System.out.println("url :"+url);
		// System.out.println("time :"+time);
		// System.out.println("guess :"+guess);
		// System.out.println("playCount :"+playCount);
		// System.out.println("Comment :"+commentNum);

		// / 存入 videoinfo开始
		ArrayList<TextValue> values2 = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = fatherid + "+tx";
		values2.add(rowkeytv);

		title = cleanString(title);
		TextValue nametv = new TextValue();
		nametv.text = "name";
		nametv.value = title;
		values2.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "tx";
		values2.add(namewebsite);

		TextValue crawltimetv = new TextValue();
		crawltimetv.text = "crawltime";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = new Date();
		crawltimetv.value = sdf.format(d);
		values2.add(crawltimetv);

		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = fatherid;
		values2.add(inforowkeytv);

		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = cid;
		values2.add(playrowkeytv);

		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = "正片";
		values2.add(showtypetv);
		
		jdbconn.insert(values2, "videoinfo");
		// /存入videoinfo结束

		// /存入videodynamic 开始
		ArrayList<TextValue> values3 = new ArrayList<TextValue>();

		values3.add(rowkeytv);
		values3.add(inforowkeytv);
		values3.add(playrowkeytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = playCount;
		values3.add(sumPlayCounttv);

		TextValue uptv = new TextValue();
		uptv.text = "up";
		uptv.value = -1;
		values3.add(uptv);

		TextValue downtv = new TextValue();
		downtv.text = "down";
		downtv.value = -1;
		values3.add(downtv);

		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// n 2:y
		values3.add(flagtv);

		TextValue commenttv = new TextValue();
		commenttv.text = "comment";
		commenttv.value = commentNum;// n 2:y
		values3.add(commenttv);

		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = time.substring(0,10);
		values3.add(timestamptv);

		values3.add(namewebsite);

		TextValue collect = new TextValue();
		collect.text = "collect";
		collect.value = -1;
		values3.add(collect);

		TextValue outside = new TextValue();
		outside.text = "outside";
		outside.value = -1;
		values3.add(outside);

		String sd = sdf.format(new Date(Long.parseLong(time )));
		jdbconn.insert(values3, "videodynamic" + sd);

		values = null;
		sdf = null;
		// /存入videodynamic 结束

	}

	public static String playGuess(String cainixihuan) {
		StringBuffer guess = new StringBuffer();
		int cai = cainixihuan.indexOf("<playurl>");
		while (cai > 0) {
			int urlEnd = cainixihuan.indexOf("</playurl>");
			String url = cainixihuan.substring(cai + 15, urlEnd - 4);
			int aname = cainixihuan.indexOf("<subtitle>", cai);
			int bname = cainixihuan.indexOf("</subtitle>", cai);
			if (aname < 0) {
				aname = cainixihuan.indexOf("<title>", cai);
				bname = cainixihuan.indexOf("</title>", cai);
				if (aname >= bname) {
					break;
				} else {
					String guessName = "";
					try {
						guessName = cainixihuan.substring(aname + 7, bname);
					} catch (Exception e) {
						e.printStackTrace();
					}
					guess = guess.append("$" + url);
					guess = guess.append("@" + guessName);
					cainixihuan = cainixihuan.substring(bname);
					cai = cainixihuan.indexOf("<playurl>");
					continue;
				}
			}
			if (aname >= bname) {
				break;
			}
			guess = guess.append("$" + url);
			String guessName = cainixihuan.substring(aname + 16, bname - 5);
			guess = guess.append("@" + guessName);
			cainixihuan = cainixihuan.substring(bname);
			cai = cainixihuan.indexOf("<playurl>");
		}
		String answer = guess.toString();
		answer = answer.replaceAll("&middot;", ".");
		return answer;
	}

	public static String analyse(String page, String begin, String end, int x) {
		String answer = "";
		int a = page.indexOf(begin);
		if (a < 0) {
			return answer;
		}
		int b = page.indexOf(end, a + x);
		if (a < b && (b - a) < 1000) {
			answer = page.substring(a + x, b);
		}
		return answer;
	}

	public static String playFragment(String page) {
		StringBuffer sb = new StringBuffer();
		Document d = Jsoup.parse(page);
		Elements links = d.getElementsByAttributeValue("_hot",
				"cover3.zhengpian.short");
		for (Element link : links) {
			// String href = link.attr("abs:href"); //不知到为什么abs用不了
			String href = link.attr("href");
			String url = "http://v.qq.com" + href;
			String title = link.attr("title");
			sb.append("$" + url);
			sb.append("@" + title);
		}

		// Document d = pag
		String answer = sb.toString();
		sb = null;
		return answer;
	}

	public static String playDuration(String page) {
		/*
		 * <meta itemprop="duration" content="1297秒"/> 1297秒
		 */
		String answer = "";
		int a = page.indexOf("duration\" content=\"");
		int b = page.indexOf("\" />", a);
		try {
			answer = page.substring(a + 19, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playPic(String page) {
		/*
		 * pic :
		 * "http://i.gtimg.cn/qqlive/img/jpgcache/files/qqvideo/p/pvhtgkz0s0uyaey.jpg"
		 * ,
		 */
		String answer = "";
		int a = page.indexOf("pic :\"");
		int b = page.indexOf("\",", a + 6);
		if (a < 0) {
			return answer;
		} else {
			answer = page.substring(a + 6, b);
		}
		return answer;
	}

	public static String playBrief(String page) {
		/*
		 * brief : "王栎鑫经纪人耍大牌：只住五星酒店",
		 */
		String answer = "";
		int a = page.indexOf("brief : \"");
		int b = page.indexOf("\",", a + 9);
		if (a < 0) {
			return answer;
		} else if (a < b && (b - a) < 500) {
			try {
				answer = page.substring(a + 9, b);
			} catch (Exception e) {

			}
		}
		return answer;
	}

	public static String playName(String page) {
		/*
		 * var COVER_INFO = { title :"有料",
		 */
		String answer = "";
		int a = page.indexOf("var COVER_INFO = {title :\"");
		int b = page.indexOf("\",", a);
		if (a < 0) {
			return answer;
		} else if (b > a && (b - a) < 500) {
			answer = page.substring(a + 26, b);
		}
		return answer;
	}

	public static String playUpdatePeriod(String page) {
		/*
		 * <meta itemprop="updatePeriod" content="周三 10:00" /> 周三 10:00
		 */
		String answer = "";
		int a = page.indexOf("updatePeriod\" content=\"");
		int b = page.indexOf("\" />", a);
		try {
			answer = page.substring(a + 23, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playTitle(String page) {
		/*
		 * <meta itemprop="name" name="title"
		 * content="Oh My思密达 暴尴尬瞬间：金基立熊抱女星现生理反应 4 minute队长跳舞肩带甩出" /> Oh My思密达
		 * 暴尴尬瞬间：金基立熊抱女星现生理反应 4 minute队长跳舞肩带甩出
		 */
		String answer = "";
		int a = page.indexOf("e\" name=\"title\" content=\"");
		int b = page.indexOf("\" />", a);
		try {
			answer = page.substring(a + 25, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playSecTitle(String page) {
		/*
		 * 文件开头：第一届韩美林艺术论坛 公共空间的艺术审美 <type>z</type>，应该有3个空格 第一届韩美林艺术论坛 公共空间的艺术审美
		 */
		String answer = "";
		int b = page.indexOf("   <type>");
		try {
			answer = page.substring(0, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playDate(String page) {
		// <datee>2014-01-05</datee>
		String answer = "";
		int a = page.indexOf("<datee>");
		int b = page.indexOf("</datee>");
		try {
			answer = page.substring(a + 7, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playUrl(String page) {
		// <thisisurl>http://v.qq.com/cover/l/lnk28qkixsseynf.html<thisisurl>
		String answer = "";
		int a = page.indexOf("<thisisurl>");
		int b = page.indexOf("</thisisurl>");
		try {
			answer = page.substring(a + 11, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playCid(String page) {
		// <cid>lnk28qkixsseynf</cid>
		String answer = "";
		int a = page.indexOf("<cid>");
		int b = page.indexOf("</cid>");
		try {
			answer = page.substring(a + 5, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String playContentLocation(String page) {
		// <meta itemprop="contentLocation" content="内地"/>
		String answer = "";
		int a = page.indexOf("contentLocation");
		int b = page.indexOf("\" />", a + 26);
		if (a < b && (b - a) < 100) {
			answer = page.substring(a + 26, b);
		}
		return answer;
	}

	public static String playInLanguage(String page) {
		// <meta itemprop="inLanguage" content="普通话"/>
		String answer = "";
		int a = page.indexOf("inLanguage\" content=\"");
		int b = page.indexOf("\" />", a);
		try {
			answer = page.substring(a + 21, b);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}

	public static String cleanString(String str) {
		str = str.replaceAll(" ", "");
		str = str.replaceAll("'", "");
		str = str.replaceAll("-", "");
		str = str.replaceAll("\n", "");
		str = str.replaceAll("\r", "");
		str = str.replaceAll("//s", "");
		return str;
	}

}
