package ParserData.TencentParserData;

import hbase.HBaseCRUD;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import Utils.JDBCConnection;
import Utils.TextValue;

public class playParserTV {
	public JDBCConnection jdbconn;
	public HBaseCRUD hbase;
	public playParserTV(HBaseCRUD hbase,JDBCConnection jdbconn){
		this.jdbconn = jdbconn;
		this.hbase = hbase;
	}
	public  void analyze(String content, int mtype, String cId,
			String vId) {
		if (mtype < 0) {
			if (judge(content).equals("cartoon")) {
				cartoonInfo(content, mtype, cId, vId);
			} else if (judge(content).equals("movie")) {
				// 对应的是电影
				if (content.contains("好莱坞影院 - 腾讯视频")) {
					movieInfo(content, mtype, cId, vId);
				} else {
					mtype = 0; // 0表示电影
					tvInfo(content, mtype, cId, vId);
				}
			} else {
				// 对应的是tv
				mtype = 1; // 1表示电视剧
				tvInfo(content, mtype, cId, vId);
			}
		} else {
			play(content, cId, vId);
		}
	}

	public static String getShijianchuo(String page) {
		String answer = "";
		int a = page.indexOf("<shijianchuo>");
		int b = page.indexOf("</shijianchuo>");
		if (a > 0 && b > a) {
			answer = page.substring(a + 13, b);
		}
		return answer;
	}

	public static String get(String page, String begin, String end, int x) {
		String answer = "";
		int a = page.indexOf(begin);
		if (a > 0) {
			int b = page.indexOf(end, a + x);
			if (b > 0 && a + x < b) {
				answer = page.substring(a + x, b);
			}
		} else {
			answer = "";
		}
		return answer;
	}

	public static String getAll(String page, String begin, String end,
			String start, String stop, int x) {
		StringBuffer sb = new StringBuffer();
		int kaishi = page.indexOf(begin);
		int jieshu = page.indexOf(end, kaishi);
		if (kaishi > 0 && jieshu > kaishi) {
			String content = page.substring(kaishi, jieshu);
			int a = content.indexOf(start);
			while (a > 0) {
				int b = content.indexOf(stop, a + x);
				if (b > 0) {
					String answer = content.substring(a + x, b);
					sb.append("@" + answer);
					a = content.indexOf(a, b);
				} else {
					sb.append("");
					break;
				}
			}
		}
		return sb.toString();
	}

	public void cartoonInfo(String details, int mtype, String cId,
			String vId) {
		String leixing = ""; // 判断是movie cartoon or tv
		String MovieName = "";
		String MovieYear = "";
		String MovieCountry = "";
		StringBuilder MovieDirector = new StringBuilder();
		StringBuilder MovieActor = new StringBuilder();
		String MovieType = ""; // 惊悚，悬疑，爱情，动作
		String Intro = "";
		String PlayCount = "";
		// String comCount = ""; // 评论数
		String Grade = "";
		// StringBuilder MovieLabel = new StringBuilder();
		// String up = "";
		// String down = "";
		String duration = ""; // shichang
		String pictureURL = "";
		String time = "";
		String othername = "";
		String guess = "";
		String movieUrl = "";
		// String shijianchuo = "";
		String language = "";
		leixing = "dongman";
		// 评分
		Grade = details.substring(0, 3);
		// 时间戳
		time = getShijianchuo(details);
		// 名称
		MovieName = get(details, "title :\"", "\",", 8);

		pictureURL = get(details, "pic :", "\",", 6);

		language = get(details, "inLanguage", "\" ", 21);
		movieUrl = get(details, "<thisisurl>", "</thisisurl>", 11);
		// 类型
		MovieType = getAll(details, "类型：", "</div>", "title=", "\">", 7);
		// 时常
		duration = get(details, "duration:\"", "\"", 10);
		// 别名
		othername = get(details, "secTitle : \"", "\",", 12);
		// 国家
		int Country = details.indexOf("地区：");
		if (Country > 0) {
			int CountryBegin = details.indexOf("title=", Country);
			int CountryEnd = details.indexOf("\">", CountryBegin);
			MovieCountry = details.substring(CountryBegin + 7, CountryEnd);
			if (MovieCountry.length() > 3) {
				MovieCountry = "无";
			}
		} else
			MovieCountry = "无";

		int Year = details.indexOf("年份：");
		// 年份
		if (Year > 0) {
			int YearBegin = details.indexOf("title=", Year);
			int YearEnd = details.indexOf("\">", YearBegin);
			MovieYear = details.substring(YearBegin + 7, YearEnd);
		} else
			MovieYear = "";

		// 简介
		Intro = get(details, "description\">", "</span>", 13);
		Intro = Intro.replaceAll("&middot;", ".");
		Intro = Intro.replaceAll("<br />", "");
		Intro = Intro.replaceAll("&quot", "");
		// 播放数
		PlayCount = get(details, "<all>     ", "    </all>", 9);
		if(PlayCount.equals("")){
			jdbconn.log("石嘉帆", cId + "+tx", 1, "tx", movieUrl, "无播放量", 2);
		}
		// 猜你喜欢
		guess = getGuess(details);

		// System.out.println("cartooninfo");
		// System.out.println(cId);
		// System.out.println("year： " + MovieYear);
		// System.out.println("name: " + MovieName);
		// System.out.println("pic: " + pictureURL);
		// System.out.println("score: " + Grade);
		// System.out.println("area: " + MovieCountry);
		// System.out.println("language :" + language);
		System.out.println("type: " + MovieType.toString());
		// System.out.println("director: " + MovieDirector.toString());
		// System.out.println("actor: " + MovieActor.toString());
		// System.out.println("leiixng: " + leixing);
		// System.out.println("Intro: " + Intro);
		// System.out.println("othername: " + othername);
		System.out.println("guess： " + guess);
		System.out.println("count: " + PlayCount);
		// System.out.println("duration: " + duration);
		// System.out.println("movieUrl: " + movieUrl);
		// System.out.println("shijianchuo: " + shijianchuo);

		String key1 = cId + "+" + "tx";
		String key2 = cId + "+" + "tx" + "+" + time;
		// String url = "http://www.letv.com/comic/" + rowkey + ".html";
		String[] rows = new String[] { key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1, key1, key1, key1 };
		String[] colfams = new String[] { "R", "R", "R", "B", "B", "B", "B",
				"B", "B", "B", "B", "B", "B", "B", "B" };
		String[] quals = new String[] { "inforowkey", "year", "website",
				"name", "pictureURL", "area", "lan", "type", "director",
				"mainactor", "category", "summarize", "othername", "duration",
				"url" };
		String[] values = new String[] { cId, MovieYear, "tx", MovieName,
				pictureURL, MovieCountry, language, MovieType.toString(),
				MovieDirector.toString(), MovieActor.toString(), leixing,
				Intro, othername, duration, movieUrl };
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

		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "C", "C", "C", "C", "C" };
		quals = new String[] { "inforowkey", "website", "timestamp", "name",
				"score", "sumplaycount", "tengxunrelated", "free" };
		values = new String[] { cId, "tx", time, MovieName, Grade, PlayCount,
				guess.toString(), "1" };
		try {
			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

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
		int yearint = -1;
		if(!MovieYear.equals("")){
			yearint = Integer.parseInt(MovieYear);
		}
		yeartv.value = yearint;
		values2.add(yeartv);

		if (Intro != "") {
			TextValue summarizetv = new TextValue();
			summarizetv.text = "summarize";
			Intro = cleanString(Intro);
			if (Intro.length() > 999) {
				Intro = Intro.substring(0, 999);
			}
			summarizetv.value = Intro;
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

		if (!language.equals("")) {
			TextValue lantv = new TextValue();
			lantv.text = "lan";
			lantv.value = language;
			values2.add(lantv);
		}

		if (!MovieCountry.equals("")) {
			TextValue areatv = new TextValue();
			areatv.text = "area";
			int index = MovieCountry.indexOf("@");
			if (index >= 0)
				MovieCountry = MovieCountry.substring(0, index);
			if (MovieCountry.contains("北美")) {
				MovieCountry = "北美";
			}
			areatv.value = MovieCountry;
			values2.add(areatv);
		}

		String directorString = MovieDirector.toString();
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
		rowkeytv.value = cId + "+tx";
		values2.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		MovieName = cleanString(MovieName);
		nametv.value = MovieName;
		values2.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "tx";
		values2.add(namewebsite);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		if (leixing.contains("cart")) {
			leixing = "dongman";
		}
		categorytv.value = leixing;
		values2.add(categorytv);

		// movietype:动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线
		// cartoontype:经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载
		// zongyitype：综合访谈选秀搞笑情感脱口秀职场游戏<歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场
		String totalType = "动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载综合访谈选秀搞笑情感脱口秀职场游戏歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场";
		int k = 0;
		String[] typeSplits = null;
		String type = MovieType.toString();
		if (type != null) {
			String typeString = new String(type);
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

		String[] mainactorSplits = null;
		String maStr = MovieActor.toString();
		if (maStr != null) {
			int index = maStr.indexOf("@");
			if (index >= 0) {
				maStr = maStr.substring(1);
			}
			mainactorSplits = maStr.split("@");
		}
		if (maStr != null) {
			for (int i = 0; i < mainactorSplits.length; i++) {
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				mainactorSplits[i] = cleanString(mainactorSplits[i]);
				if (mainactorSplits[i].length() > 20) {
					break;
				}
				actortv.value = mainactorSplits[i];
				values2.add(actortv);
				if (i == 2)
					break;
			}
		}

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		if (duration.equals(""))
			duration = "-1";
		if (duration.length() > 19) {
			duration = "-1";
		}
		duratv.value = duration;
		values2.add(duratv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = "-1";
		values2.add(pricetv);
		if(!existMovieinfo(cId+ "+tx")){
			jdbconn.insert(values2, "movieinfo");
		}
		// 存入mysql 结束

		// / 存入mysql moviedynamic开始
		double scoredouble = -1;
		if (Grade != null) {
			try {
				scoredouble = Double.parseDouble(Grade);
			} catch (Exception e) {
			}
		}

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
		scoretv.value = scoredouble;
		values3.add(scoretv);

		values3.add(categorytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = PlayCount;
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
		if(!existMoviedynamic(cId+ "+tx" , sd)){
			jdbconn.insert(values3, "moviedynamic" + sd);
		}
		// 存入 mysql moviedynamic 结束

	}

	public  void movieInfo(String details, int mtype, String cId,
			String vId) {
		String leixing = ""; // 判断是movie cartoon or tv
		String MovieName = "";
		String MovieYear = "";
		String MovieCountry = "";
		String MovieDirector = "";
		String MovieActor = "";
		String MovieType = ""; // 惊悚，悬疑，爱情，动作
		String Intro = "";
		String PlayCount = "";
		// String comCount = ""; // 评论数
		String Grade = "";
		// StringBuilder MovieLabel = new StringBuilder();
		String duration = ""; // shichang
		String pictureURL = "";
		String othername = "";
		String guess = "";
		String movieUrl = "";
		// String shijianchuo = "";
		String time = "";
		String language = "";

		Grade = details.substring(0, 3);
		leixing = "movie";
		// 时间戳
		time = getShijianchuo(details); // 直接从time得到，不需要解析
		movieUrl = get(details, "<thisisurl>", "</thisisurl>", 11);

		MovieName = get(details, "title :\"", "\",", 8);
		othername = get(details, "secTitle :\"", "\",", 12);
		pictureURL = get(details, "pic :\"", "\",", 6);
		duration = get(details, "duration:\"", "\",", 10);
		MovieActor = getAll(details, "主演：<", "</dd>", "title=", "\">", 7);
		MovieActor = MovieActor.replaceAll("&middot;", ".");

		MovieDirector = getAll(details, "导演：<", "</span>", "title=", "\">", 7);
		MovieDirector = MovieDirector.replaceAll("&middot;", ".");

		MovieType = getAll(details, "类型：", "</span>", "bread_1\">", "</a>", 9);
		MovieCountry = get(details, "地区：", "title=", "\">", 6);
		MovieYear = get(details, "年份：", "title=", "\">", 7);

		Intro = get(details, "moredesc_cut\":\"", "\"};", 15);
		Intro = Intro.replaceAll("&middot;", ".");
		Intro = Intro.replaceAll("<br />", "");
		Intro = Intro.replaceAll("&quot", "");

		PlayCount = get(details, "<all>     ", "    </all>", 10);
		if(PlayCount.equals("")){
			jdbconn.log("石嘉帆", cId + "+tx", 1, "tx", movieUrl, "无播放量", 2);
		}
		guess = getGuess(details);

		// System.out.println("movieinfo");
		// System.out.println(cId);
		System.out.println("myear:" + MovieYear);
		// System.out.println("name: " + MovieName);
		// System.out.println("pic: " + pictureURL);
		// System.out.println("score: " + Grade);
		// System.out.println("area: " + MovieCountry);
		// System.out.println("language :" + language);
		// System.out.println("type: " + MovieType.toString());
		// System.out.println("director: " + MovieDirector.toString());
		// System.out.println("actor: " + MovieActor.toString());
		// System.out.println("leiixng: " + leixing);
		// System.out.println("Intro: " + Intro);
		// System.out.println("othername: " + othername);
		// System.out.println("guess： " + guess);
		// System.out.println("count: " + PlayCount);
		// System.out.println("duration: " + duration);
		// System.out.println("movieUrl: " + movieUrl);
		// System.out.println("shijianchuo: " + time);

		String key1 = cId + "+" + "tx";
		String key2 = cId + "+" + "tx" + "+" + time;
		// String url = "http://www.letv.com/comic/" + rowkey + ".html";
		String[] rows = new String[] { key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1, key1, key1, key1 };
		String[] colfams = new String[] { "R", "R", "R", "B", "B", "B", "B",
				"B", "B", "B", "B", "B", "B", "B", "B" };
		String[] quals = new String[] { "inforowkey", "year", "website",
				"name", "pictureURL", "area", "lan", "type", "director",
				"mainactor", "category", "summarize", "othername", "duration",
				"url" };
		String[] values = new String[] { cId, MovieYear, "tx", MovieName,
				pictureURL, MovieCountry, language, MovieType.toString(),
				MovieDirector.toString(), MovieActor.toString(), leixing,
				Intro, othername, duration, movieUrl };
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
		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "C", "C", "C", "C", "C" };
		quals = new String[] { "inforowkey", "website", "timestamp", "name",
				"score", "sumplaycount", "tengxunrelated", "free" };
		values = new String[] { cId, "tx", time, MovieName, Grade, PlayCount,
				guess.toString(), "0" };
		try {
			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

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
		int yearint = -1;
		if(!MovieYear.equals("")){
			yearint = Integer.parseInt(MovieYear);
		}
		yeartv.value = yearint;
		values2.add(yeartv);

		if (Intro != "") {
			TextValue summarizetv = new TextValue();
			summarizetv.text = "summarize";
			Intro = cleanString(Intro);
			if (Intro.length() > 999) {
				Intro = Intro.substring(0, 999);
			}
			summarizetv.value = Intro;
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

		if (!language.equals("")) {
			TextValue lantv = new TextValue();
			lantv.text = "lan";
			lantv.value = language;
			values2.add(lantv);
		}

		if (!MovieCountry.equals("")) {
			TextValue areatv = new TextValue();
			areatv.text = "area";
			int index = MovieCountry.indexOf("@");
			if (index >= 0)
				MovieCountry = MovieCountry.substring(0, index);
			if (MovieCountry.contains("北美")) {
				MovieCountry = "北美";
			}
			areatv.value = MovieCountry;
			values2.add(areatv);
		}

		String directorString = MovieDirector.toString();
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
		rowkeytv.value = cId + "+tx";
		values2.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		MovieName = cleanString(MovieName);
		nametv.value = MovieName;
		values2.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "tx";
		values2.add(namewebsite);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		if (leixing.contains("cart")) {
			leixing = "dongman";
		}
		categorytv.value = leixing;
		values2.add(categorytv);

		// movietype:动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线
		// cartoontype:经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载
		// zongyitype：综合访谈选秀搞笑情感脱口秀职场游戏<歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场
		String totalType = "动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载综合访谈选秀搞笑情感脱口秀职场游戏歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场";
		int k = 0;
		String[] typeSplits = null;
		String type = MovieType.toString();
		if (type != null) {
			String typeString = new String(type);
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

		String[] mainactorSplits = null;
		String maStr = MovieActor.toString();
		if (maStr != null) {
			int index = maStr.indexOf("@");
			if (index >= 0) {
				maStr = maStr.substring(1);
			}
			mainactorSplits = maStr.split("@");
		}
		if (maStr != null) {
			for (int i = 0; i < mainactorSplits.length; i++) {
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				mainactorSplits[i] = cleanString(mainactorSplits[i]);
				if (mainactorSplits[i].length() > 20) {
					break;
				}
				actortv.value = mainactorSplits[i];
				values2.add(actortv);
				if (i == 2)
					break;
			}
		}

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		if (duration.equals(""))
			duration = "-1";
		if (duration.length() > 19) {
			duration = "-1";
		}
		duratv.value = duration;
		values2.add(duratv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = "-1";
		values2.add(pricetv);
		if(!existMovieinfo(cId+ "+tx")){
			jdbconn.insert(values2, "movieinfo");
		}
		// 存入mysql 结束

		// / 存入mysql moviedynamic开始
		double scoredouble = -1;
		if (Grade != null) {
			try {
				scoredouble = Double.parseDouble(Grade);
			} catch (Exception e) {
			}
		}

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
		freetv.value = 0;
		values3.add(freetv);

		TextValue women = new TextValue();
		women.text = "women";
		women.value = -1;
		values3.add(women);

		TextValue scoretv = new TextValue();
		scoretv.text = "score";
		scoretv.value = scoredouble;
		values3.add(scoretv);

		values3.add(categorytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = PlayCount;
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
		if(!existMoviedynamic(cId+ "+tx",sd)){
			jdbconn.insert(values3, "moviedynamic" + sd);
		}
		// 存入 mysql moviedynamic 结束

	}

	public  void tvInfo(String details, int mtype, String cId,
			String vId) {
		String leixing = ""; // 判断是movie cartoon or tv
		String MovieName = "";
		String MovieYear = "";
		String MovieCountry = "";
		StringBuilder MovieDirector = new StringBuilder();
		StringBuilder MovieActor = new StringBuilder();
		StringBuilder MovieType = new StringBuilder(); // 惊悚，悬疑，爱情，动作
		String Intro = "";
		String PlayCount = "";
		// String comCount = ""; // 评论数
		String Grade = "";
		// StringBuilder MovieLabel = new StringBuilder(); //目前是把label写入到type里面
		String duration = ""; // shichang
		String pictureURL = "";
		String othername = "";
		String guess = "";
		String movieUrl = "";
		// String shijianchuo = "";
		String time = "";
		String language = "";

		Grade = details.substring(0, 3);
		// 时间戳
		time = getShijianchuo(details);
		leixing = "movie";
		if (mtype == 1) {
			leixing = "tv";
		}
		movieUrl = get(details, "<thisisurl>", "</thisisurl>", 11);
		MovieName = get(details, "title :\"", "\",", 8);
		// othername
		othername = get(details, "secTitle :", "\",", 12);
		duration = get(details, "duration:\"", "\",", 10);
		language = get(details, "inLanguage", "\" ", 21);
		MovieYear = get(details, "<thisisyear>", "</thisisyear>", 12)
				.replaceAll("\\D+", "");

		// pictureURL
		pictureURL = get(details, "pic :", "\",", 6);
		// 主演
		int Actor = details.indexOf("主演：<");
		int Director = details.indexOf("导演：<");
		int type = details.indexOf("标签："); // 原来是“类型：”
		int Year = details.indexOf("年份：");
		// int Country = details.indexOf("地区：");

		if (Actor > 0) {
			int Actorjieshu = details.indexOf("</div>", Actor);
			String Actorlist = details.substring(Actor, Actorjieshu);
			while (Actor > 0) {
				int ActorBegin = Actorlist.indexOf("title=", 0);
				int ActorEnd = Actorlist.indexOf("span", ActorBegin + 7);
				String ActorName = "";
				if (ActorBegin + 7 < ActorEnd) {
					ActorName = Actorlist.substring(ActorBegin + 7,
							ActorEnd - 3);
				} else {
					break;
				}
				ActorName = ActorName.replaceAll("&middot;", ".");
				Actorlist = Actorlist.substring(ActorEnd);
				Actor = Actorlist.indexOf("title=");
				MovieActor = MovieActor.append("@" + ActorName);
			}
		} else {
			MovieActor = MovieActor.append("无");
		}
		// 导演
		if (Director > 0) {
			int Directorjieshu = details.indexOf("</div>", Director);
			String Directorlist = details.substring(Director, Directorjieshu);
			while (Director > 0) {
				int DirectorBegin = Directorlist.indexOf("title=", 0);
				int DirectorEnd = Directorlist.indexOf("span",
						DirectorBegin + 7);
				String DirectorName = "";
				if (DirectorBegin > 0 && DirectorEnd > 0
						&& DirectorEnd > DirectorBegin) {
					DirectorName = Directorlist.substring(DirectorBegin + 7,
							DirectorEnd - 3);
				} else {
					break;
				}
				DirectorName = DirectorName.replaceAll("&middot;", ".");
				Directorlist = Directorlist.substring(DirectorEnd);
				MovieDirector = MovieDirector.append("@" + DirectorName);
				Director = Directorlist.indexOf("title=");
			}
		} else {
			MovieDirector = MovieDirector.append("无");
		}

		// 类型
		if (type > 0) {
			int typejieshu = details.indexOf("</div>", type);
			String typelist = details.substring(type, typejieshu);
			while (type > 0) {
				int typeBegin = typelist.indexOf("title=", 0);
				int typeEnd = typelist.indexOf("\">", typeBegin + 7);
				if (typeBegin + 7 > typeEnd) {
					break;
				}
				String typeName = typelist.substring(typeBegin + 7, typeEnd);
				MovieType = MovieType.append("@" + typeName);
				typelist = typelist.substring(typeEnd);
				type = typelist.indexOf("title=");
			}
		} else {
			MovieType = MovieType.append("无");
		}
		/*
		 * 国家 网页格式变了，这个页不好用了
		 */
		MovieCountry = get(details, "contentLocation", "\" ", 26);

		// 年份
		/*
		 * this method is not outdate 2015.5.12 by sjf
		 * 
		 * if (Year > 0) { int YearBegin = details.indexOf("title=", Year); int
		 * YearEnd = details.indexOf("\">", YearBegin); MovieYear =
		 * details.substring(YearBegin + 7, YearEnd); } else MovieYear = "";
		 */

		/*
		 * 简介 格式变了
		 */
		Intro = get(details, "intro_full", "</p>", 12);
		Intro = Intro.replaceAll("&middot;", ".");
		Intro = Intro.replaceAll("<br />", "");
		Intro = Intro.replaceAll("&quot", "");
		// 播放数
		PlayCount = get(details, "<all>     ", "    </all>", 10);
		if(PlayCount.equals("")){
			jdbconn.log("石嘉帆", cId + "+tx", 1, "tx", movieUrl, "无播放量", 2);
		}
		// 猜你喜欢
		guess = getGuess(details);
		// 猜你喜欢
		// System.out.println("movieinfo");
		// System.out.println(cId);
		System.out.println("year：" + MovieYear);
		// System.out.println("name: " + MovieName);
		// // System.out.println("pic: " + pictureURL);
		// // System.out.println("score: " + Grade);
		// // System.out.println("area: " + MovieCountry);
		// // System.out.println("language :" + language);
		// System.out.println("type: " + MovieType.toString());
		// // System.out.println("director: " + MovieDirector.toString());
		// // System.out.println("actor: " + MovieActor.toString());
		// System.out.println("leiixng: " + leixing);
		// // System.out.println("Intro: " + Intro);
		// // System.out.println("othername: " + othername);
		// // System.out.println("guess： " + guess);
		// System.out.println("count: " + PlayCount);
		// System.out.println("duration: " + duration);
		// System.out.println("movieUrl: " + movieUrl);
		// System.out.println("shijianchuo: " + time);

		String key1 = cId + "+" + "tx";
		String key2 = cId + "+" + "tx" + "+" + time;
		// String url = "http://www.letv.com/comic/" + rowkey + ".html";
		String[] rows = new String[] { key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1, key1, key1, key1 };
		String[] colfams = new String[] { "R", "R", "R", "B", "B", "B", "B",
				"B", "B", "B", "B", "B", "B", "B", "B" };
		String[] quals = new String[] { "inforowkey", "year", "website",
				"name", "pictureURL", "area", "lan", "type", "director",
				"mainactor", "category", "summarize", "othername", "duration",
				"url" };
		String[] values = new String[] { cId, MovieYear, "tx", MovieName,
				pictureURL, MovieCountry, language, MovieType.toString(),
				MovieDirector.toString(), MovieActor.toString(), leixing,
				Intro, othername, duration, movieUrl };
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

		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "C", "C", "C", "C", "C" };
		quals = new String[] { "inforowkey", "website", "timestamp", "name",
				"score", "sumplaycount", "tengxunrelated", "free" };
		values = new String[] { cId, "tx", time, MovieName, Grade, PlayCount,
				guess.toString(), "1" };
		try {
			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// 存入mysql movieinfo 开始
		ArrayList<TextValue> values2 = new ArrayList<TextValue>();

		TextValue crawltimetv = new TextValue();
		crawltimetv.text = "crawltime";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date d = new Date();
		crawltimetv.value = sdf.format(d);
		values2.add(crawltimetv);
       
		TextValue yeartv = new TextValue();
		yeartv.text = "year";
		int yearint = -1;
		if(!MovieYear.equals("")){
			yearint = Integer.parseInt(MovieYear);
		}
		yeartv.value = yearint;
		values2.add(yeartv);
       

		if (Intro != "") {
			TextValue summarizetv = new TextValue();
			summarizetv.text = "summarize";
			Intro = cleanString(Intro);
			if (Intro.length() > 999) {
				Intro = Intro.substring(0, 999);
			}
			summarizetv.value = Intro;
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

		if (!language.equals("")) {
			TextValue lantv = new TextValue();
			lantv.text = "lan";
			lantv.value = language;
			values2.add(lantv);
		}

		if (!MovieCountry.equals("")) {
			TextValue areatv = new TextValue();
			areatv.text = "area";
			int index = MovieCountry.indexOf("@");
			if (index >= 0)
				MovieCountry = MovieCountry.substring(0, index);
			if (MovieCountry.contains("北美")) {
				MovieCountry = "北美";
			}
			areatv.value = MovieCountry;
			values2.add(areatv);
		}

		String directorString = MovieDirector.toString();
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
		rowkeytv.value = cId + "+tx";
		values2.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		MovieName = cleanString(MovieName);
		nametv.value = MovieName;
		values2.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "tx";
		values2.add(namewebsite);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		if (leixing.contains("cart")) {
			leixing = "dongman";
		}
		categorytv.value = leixing;
		values2.add(categorytv);

		// movietype:动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线
		// cartoontype:经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载
		// zongyitype：综合访谈选秀搞笑情感脱口秀职场游戏<歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场
		String totalType = "动作冒险喜剧爱情战争恐怖犯罪悬疑惊悚武侠科幻音乐歌舞动画奇幻家庭剧情伦理记录历史传记院线经典少男少女萌系耽美搞笑惊悚魔幻科幻推理儿童音乐儿童益智儿童教育儿童历险儿童奇幻儿童搞笑儿童竞技原创真人其他预告片特辑连载综合访谈选秀搞笑情感脱口秀职场游戏歌舞美食文化少儿腾讯出品纪实旅游演唱会生活曲艺欢乐派对真人秀场";
		int k = 0;
		String[] typeSplits = null;
		String typeStr = MovieType.toString();
		if (typeStr != null) {
			if (typeStr.contains("@")) {
				try {
					typeStr = typeStr.substring(1);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			typeSplits = typeStr.split("@");
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

		String[] mainactorSplits = null;
		String maStr = MovieActor.toString();
		if (maStr != null) {
			int index = maStr.indexOf("@");
			if (index >= 0) {
				maStr = maStr.substring(1);
			}
			mainactorSplits = maStr.split("@");
		}
		if (maStr != null) {
			for (int i = 0; i < mainactorSplits.length; i++) {
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				mainactorSplits[i] = cleanString(mainactorSplits[i]);
				if (mainactorSplits[i].length() > 20) {
					break;
				}
				actortv.value = mainactorSplits[i];
				values2.add(actortv);
				if (i == 2)
					break;
			}
		}

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		if (duration.equals(""))
			duration = "-1";
		if (duration.length() > 19) {
			duration = "-1";
		}
		duratv.value = duration;
		values2.add(duratv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = "-1";
		values2.add(pricetv);
		if(!existMovieinfo(cId+ "+tx")){
			jdbconn.insert(values2, "movieinfo");
		}
		// / 存入mysql movieinfo 结束

		// / 存入mysql moviedynamic开始
		double scoredouble = -1;
		if (Grade != null) {
			try {
				scoredouble = Double.parseDouble(Grade);
			} catch (Exception e) {
			}
		}

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
		scoretv.value = scoredouble;
		values3.add(scoretv);

		values3.add(categorytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = PlayCount;
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
		if(!existMoviedynamic(cId+ "+tx",sd)){
			jdbconn.insert(values3, "moviedynamic" + sd);
		}
		// 存入 mysql moviedynamic 结束

	}

	public  void play(String totalDetails, String cId, String vId) {
		String MovieName = "";
		String movieUrl = "";
		String PlayCount = "";
		String up = "";
		String down = "";
		String comCount = "";
		String time = "";

		// 名字
		int BeginName = totalDetails.indexOf("   <type>");
		if (BeginName > 0 && BeginName < 500) {
			MovieName = totalDetails.substring(0, BeginName);
		}
		time = getShijianchuo(totalDetails);
		movieUrl = get(totalDetails, "<thisisurl>", "</thisisurl>", 11);
		PlayCount = get(totalDetails, "<all>     ", "    </all>", 10);
		if(PlayCount.equals("")){
			jdbconn.log("石嘉帆", cId + "+tx", 1, "tx", movieUrl, "无播放量", 2);
		}
		down = get(totalDetails, "<id>     -1    </id>    <num>", "    </num>",
				34);
		up = get(totalDetails, "<id>     1    </id>    <num>", "    </num>", 33);
		comCount = get(totalDetails, "commentnum", "&quot;}", 23);
		PlayCount = nullToZero(PlayCount);
		up = nullToZero(up);
		down = nullToZero(down);
		comCount = nullToZero(comCount);

		String showType = "正片";
		if (MovieName.contains("预告")) {
			showType = "预告片";
		} else if (MovieName.contains("花絮") || MovieName.contains("片段")
				|| MovieName.contains("幕后") || MovieName.contains("《")
				&& MovieName.contains("》")) {
			showType = "花絮";
		} else if (MovieName.contains("mv") || MovieName.contains("Mv")
				|| MovieName.contains("MV")) {
			showType = "mv";
		} else if (MovieName.length() > 20) {
			showType = "预告片";
		}
		// System.out.println("dynamic");
		// System.out.println(cId);
		// System.out.println(vId);
		// System.out.println(time);
		// System.out.println(MovieName);
		// System.out.println(movieUrl);
		// System.out.println("PlayCount :" + PlayCount);
		System.out.println("up :" + up);
		System.out.println("down :" + down);
		// System.out.println("comCount : " + comCount);
		// System.out.println("showType: " + showType);
		/*
		 * play to hbase
		 */
		String key3 = cId + "+" + vId + "+" + "tx";
		String key4 = cId + "+" + vId + "+" + "tx" + "+" + time;

		// System.out.println(key4);

		String[] rows = new String[] { key3, key3, key3, key3, key3, key3 };
		String[] colfams = new String[] { "R", "R", "R", "B", "B", "B" };
		String[] quals = new String[] { "inforowkey", "playrowkey", "website",
				"name", "url", "showType" };
		String[] values = new String[] { cId, vId, "tx", MovieName, movieUrl,
				showType };
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
				"timestamp", "sumplaycount", "comment", "updown" };
		values = new String[] { cId, vId, "tx", time, PlayCount, comCount,
				up + "@" + down };
		try {
			hbase.putRows("videodynamic", rows, colfams, quals, values);
			// hbase.putRows("videodynamicbak", rows, colfams, quals, values);
			hbase.putRows("videodynamicbaktx2", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		/// 存入 videoinfo开始
		ArrayList<TextValue> values2 = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = cId + "+tx";
		values2.add(rowkeytv);

		MovieName = cleanString(MovieName);
		TextValue nametv = new TextValue();
		nametv.text = "name";
		nametv.value = MovieName;
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
		inforowkeytv.value = cId;
		values2.add(inforowkeytv);

		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = vId;
		values2.add(playrowkeytv);

		if (!showType.equals("")) {
			TextValue showtypetv = new TextValue();
			showtypetv.text = "showtype";
			showtypetv.value = showType;
			values2.add(showtypetv);
		}
		if(!existVideoinfo(cId+ "+tx")){
			jdbconn.insert(values2, "videoinfo");
		}
		// /存入videoinfo结束

		// /存入videodynamic 开始
		ArrayList<TextValue> values3 = new ArrayList<TextValue>();

		values3.add(rowkeytv);
		values3.add(inforowkeytv);
		values3.add(playrowkeytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = PlayCount;
		values3.add(sumPlayCounttv);

		TextValue uptv = new TextValue();
		uptv.text = "up";
		uptv.value = up;
		values3.add(uptv);

		TextValue downtv = new TextValue();
		downtv.text = "down";
		downtv.value = down;
		values3.add(downtv);

		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// n 2:y
		values3.add(flagtv);

		TextValue commenttv = new TextValue();
		commenttv.text = "comment";
		commenttv.value = comCount;// n 2:y
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

	public static String judge(String page) {
		if (page.indexOf("<type>c</type>") > 0) {
			return "cartoon";
		} else if (page.indexOf("<type>t</type>") > 0) {
			return "tv";
		} else {
			return "movie";
		}
	}

	public static String getGuess(String details) {
		String cainixihuan = "";
		StringBuffer sb = new StringBuffer();
		int pianduanEnd = details.indexOf("--猜你喜欢---");
		if (pianduanEnd > 0) {
			cainixihuan = details.substring(pianduanEnd);
		} else {
			return "";
		}
		int cai = cainixihuan.indexOf("<playurl>");
		while (cai > 0) {
			int urlEnd = cainixihuan.indexOf("</playurl>");
			String url = cainixihuan.substring(cai + 15, urlEnd - 4);
			int aname = cainixihuan.indexOf("<subtitle>", cai);
			int bname = cainixihuan.indexOf("</subtitle>", cai);
			if (aname >= bname) {
				break;
			}
			sb = sb.append("$" + url);
			String guessName = cainixihuan.substring(aname + 16, bname - 5);
			sb = sb.append("@" + guessName);
			cainixihuan = cainixihuan.substring(bname);
			cai = cainixihuan.indexOf("<playurl>");
		}
		return sb.toString();
	}

	public static String nullToZero(String count) {
		if (count == "")
			count = "0";
		return count;
	}

	private static String get(String page, String begin, String start,
			String end, int x) {
		String answer = "";
		int a = page.indexOf(begin);
		if (a > 0) {
			String answerpage = page.substring(a);
			int b = answerpage.indexOf(start);
			if (b > 0) {
				int c = answerpage.indexOf(end, b + x);
				if (c > b) {
					answer = answerpage.substring(b + x, c);
				}
			}
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
	
	public boolean existMovieinfo(String rowkey){
		int count=-1;
		count=jdbconn.executeQueryCount("select count(*) as count from movieinfo where rowkey=\'"+rowkey+"\'");
		if(count > 0)
			return true;
		else 
			return false;
	}
	
	public boolean existMoviedynamic(String rowkey,String date){
		int count=-1;
		count=jdbconn.executeQueryCount("select count(*) as count from moviedynamic"+date+" where rowkey=\'"+rowkey+"\'");
		if(count > 0)
			return true;
		else
			return false;	
	}
	
	public boolean existVideoinfo(String rowkey){
		int count=-1;
		count=jdbconn.executeQueryCount("select count(*) as count from videoinfo where rowkey=\'"+rowkey+"\'");
		if(count > 0)
			return true;
		else 
			return false;
	}
	
	public boolean existVideodynamic(String rowkey,String date){
		int count=-1;
		count=jdbconn.executeQueryCount("select count(*) as count from moviedynamic"+date+" where rowkey=\'"+rowkey+"\'");
		if(count > 0)
			return true;
		else
			return false;	
	}

}
