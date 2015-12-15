package ParserData.LeshiParserData;



import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import Utils.JDBCConnection;
import Utils.TextValue;
import hbase.HBaseCRUD;

public class TVparse {
	
	public JDBCConnection jdbconn;
	public HBaseCRUD hbase;

	public String MovieName = "无";// 电影或者电视剧名称
	public String MovieYear = "无";// 年份
	public String MovieCountry = "无";// 出产地
	public String MovieDirector = "无";// 导演
	public String MovieActor = "";// 演员
	public String MovieType = "";// 类型
	public String Intro = "无";// 简介
	public String PlayCount = "";// 播放量
	public String sumPlayCount = "";// 总播放量
	public String comCount = "";// 评论数
	public String Grade = "";// 评分
	public String sumComCount = "";// 总评论数
	public String GuessMovie = "无";// 猜你喜欢
	public String key = "";
	public String Duration = "";// 时长
	public String PictureURL = "";// 图片url
	public String Name = "";// 剧集名
	public String Episode = "";// 集数
	public String album = "";
	public String upDown = "";
	public String up = "";
	public String down = "";
	
	public TVparse(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}

	public void parser(String details, String rowkey, String typeString,
			String time) throws Exception {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String now = sdf.format(new Date(Long.valueOf(time)));
		
		String url = "http://www.letv.com/tv/" + rowkey + ".html";
		int urlEnd=details.indexOf(".html");
		String playurl = details.substring(5,urlEnd+5);
		int keyBegin=details.indexOf("vplay");
		key=details.substring(keyBegin+6,urlEnd);
		
		int pai_bar=0;
		int flag=2;
		if (details.contains("play http")) {
			flag = 1;
			int NameBegin = details.indexOf("<title>");
			int NameEnd = details.indexOf("在线观看", NameBegin);
			if (NameBegin > 0 && NameEnd > 0)
				MovieName = details.substring(NameBegin + 7, NameEnd - 2);
			
			if(MovieName.isEmpty()){
				jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "未解析出视频集名称", 2);
				return;
			}
			
			int albumBegin = details.indexOf("albumtype", NameEnd);
			int albumEnd = details.indexOf(",", albumBegin);
			album = details.substring(albumBegin + 15, albumEnd - 1);

			int epiBeg = details.indexOf("totalcount:", NameEnd);
			int epiEnd = details.indexOf(",", epiBeg);
			if (epiBeg > 0 && epiEnd > 0)
				Episode = details.substring(epiBeg + 11, epiEnd);// 剧集数

			int nameBeg = details.indexOf("pTitle:\"");
			int nameEnd = details.indexOf(",", nameBeg);
			if (nameBeg > 0 && nameEnd > 0)
				Name = details.substring(nameBeg + 8, nameEnd - 1);// 剧集名
			if(Name.isEmpty()){
				jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "未解析出视频名称", 2);
				return;
			}
			int picBeg = details.indexOf("pPic:\"", nameEnd);
			int picEnd = details.indexOf(",", picBeg);
			if (picBeg > 0 && picEnd > 0)
				PictureURL = details.substring(picBeg + 6, picEnd - 1);// 图片url

			int durBeg = details.indexOf("duration:", picEnd);
			int durEnd = details.indexOf(",", durBeg);
			if (durBeg > 0 && durEnd > 0)

				Duration = details.substring(durBeg + 10, durEnd - 1);// 时长

			int intro_bar = details.indexOf("info_list", NameEnd);
			pai_bar = details.indexOf("star_box", intro_bar);
			if (intro_bar < 0 || pai_bar < 0){
				jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "未抓到动态数据", 2);
				return;
			}
			String infoDetails = details.substring(intro_bar, pai_bar);

			int Year = infoDetails.indexOf("年份");
			if (Year > 0) {
				int yearBegin = infoDetails.indexOf("_blank", Year);
				int yearEnd = infoDetails.indexOf("</a>", yearBegin);
				if (yearBegin > 0 && yearEnd > 0)
					MovieYear = infoDetails.substring(yearBegin + 8, yearEnd);
			}

			int Country = infoDetails.indexOf("地区", Year);

			if (Country > 0) {
				int conBegin = infoDetails.indexOf("_blank", Country);
				int conEnd = infoDetails.indexOf("</a>", conBegin);
				if (conBegin > 0 && conEnd > 0)
					MovieCountry = infoDetails.substring(conBegin + 8, conEnd);
			}

			int Director = infoDetails.indexOf("导演", Year);

			if (Director > 0) {
				int DirBegin = infoDetails.indexOf("_blank", Director);
				int DirEnd = infoDetails.indexOf("</a>", DirBegin);
				if (DirBegin > 0 && DirEnd > 0) {
					MovieDirector = infoDetails.substring(DirBegin + 8, DirEnd);
				}
			}

			int type = infoDetails.indexOf("类型");
			MovieType = Type(infoDetails, type);

			int IntroIndex = infoDetails.indexOf("limit_txt");
			int end = infoDetails.indexOf("class=\"more", IntroIndex);
			int IntroBegin = infoDetails.indexOf("title=", IntroIndex);
			int IntroEnd = infoDetails.indexOf("\">", IntroBegin);
			if (IntroEnd > end) {
				Intro = infoDetails.substring(IntroIndex + 11,
						infoDetails.indexOf("</p>", IntroIndex));
			} else {
				if (IntroBegin > 0 && IntroEnd > 0)
					Intro = infoDetails.substring(IntroBegin + 7, IntroEnd);
			}

		}
		int info = details.indexOf("info h", pai_bar);
		int like = details.indexOf("guessyoulike h", info);
		
		if (info > -1 && like > -1) {

			String dynamicDetails = details.substring(info, like);

			int SumCoBegin = dynamicDetails.indexOf("plist_play_count");
			int SumCoEnd = dynamicDetails.indexOf(",\"", SumCoBegin);
			if (SumCoBegin > 0 && SumCoEnd > 0)
				sumPlayCount = dynamicDetails.substring(SumCoBegin + 18,
						SumCoEnd);

			if(sumPlayCount.isEmpty()){
				jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "无总播放量", 2);
				return;
			}
			int PlayCoBegin = dynamicDetails.indexOf("media_play_count",
					SumCoEnd);
			int PlayCoEnd = dynamicDetails.indexOf(",\"", PlayCoBegin);
			if (PlayCoBegin > 0 && PlayCoEnd > 0)
				PlayCount = dynamicDetails.substring(PlayCoBegin + 18,
						PlayCoEnd);
			if(PlayCount.isEmpty()){
				jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "无分播放量", 2);
				return;
			}
			int ScoreBegin = dynamicDetails.indexOf("plist_score", PlayCoEnd);
			int ScoreEnd = dynamicDetails.indexOf(",\"", ScoreBegin);
			if (ScoreBegin > 0 && ScoreEnd > 0)
				Grade = dynamicDetails.substring(ScoreBegin + 13, ScoreEnd);

			int vcomBegin = dynamicDetails.indexOf("vcomm_count", ScoreEnd);
			int vcomEnd = dynamicDetails.indexOf(",\"", vcomBegin);
			if (vcomBegin > 0 && vcomEnd > 0)
				comCount = dynamicDetails.substring(vcomBegin + 13, vcomEnd);

			int sumComCountBeg = dynamicDetails.indexOf("pcomm_count", vcomEnd);
			int sumComCountEnd = dynamicDetails.indexOf(",\"", sumComCountBeg);
			if (sumComCountBeg > 0 && sumComCountEnd > 0)
				sumComCount = dynamicDetails.substring(sumComCountBeg + 13,
						sumComCountEnd);

			int upBeg = dynamicDetails.indexOf("up", sumComCountBeg);
			int upEnd = dynamicDetails.indexOf(",\"", upBeg);
			if (upBeg > 0 && upEnd > 0){
				upDown = dynamicDetails.substring(upBeg + 4, upEnd);
				up=dynamicDetails.substring(upBeg + 4, upEnd);
			}

			int downBeg = dynamicDetails.indexOf("down", upEnd);
			int downEnd = dynamicDetails.indexOf("}", downBeg);
			if (downBeg > 0 && downEnd > 0){
				upDown = upDown + "@"
						+ dynamicDetails.substring(downBeg + 6, downEnd);
				down=dynamicDetails.substring(downBeg + 6, downEnd);
			}

			if (flag == 1) {
				MovieDirector = DirectorScore(dynamicDetails, MovieDirector);
				MovieActor = ActorScore(dynamicDetails);
			}
			GuessMovie = YouLike(details.substring(like),now);
		} else{
			jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "未抓到动态数据", 2);
			return;
		}
		
		String[] rows;
		String[] colfams;
		String[] quals;
		String[] values;
		rowkey=rowkey.trim();
		String key1 = rowkey + "+" + "ls";
		String key2 = rowkey + "+" + "ls" + "+" + time;
		String key3 = rowkey + "+" + key + "+" + "ls";
		String key4 = rowkey + "+" + key + "+" + "ls" + "+" + time;
		key1 = key1.trim();
		key2 = key2.trim();
		key3 = key3.trim();
		key4 = key4.trim();
		if (flag == 1) {
			if (MovieActor == null){
				jdbconn.log("徐萌", rowkey+"+ls", 1, "ls", playurl, "无演员信息", 2);
				return;
			}
				
			rows = new String[] { key1, key1, key1, key1, key1, key1, key1,
					key1, key1, key1, key1, key1 };
			colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B",
					"B", "B", "B", "B" };
			quals = new String[] { "inforowkey", "year", "website", "name",
					"pictureURL", "area", "type", "director", "mainactor",
					"category", "summarize", "url" };
			values = new String[] { rowkey, MovieYear, "ls", Name, PictureURL,
					MovieCountry, MovieType.toString(), MovieDirector,
					MovieActor, typeString, Intro, url };

			hbase.putRows("movieinfo", rows, colfams, quals, values);
			hbase.putRows("movieinfobak", rows, colfams, quals, values);

			rows = new String[] { key2, key2, key2, key2, key2, key2, key2,
					key2 };
			colfams = new String[] { "R", "R", "R", "C", "C", "C", "C", "C" };
			quals = new String[] { "inforowkey", "website", "timestamp",
					"name", "score", "sumplaycount", "jishu", "comment" };
			values = new String[] { rowkey, "ls", time, Name, Grade,
					sumPlayCount, Episode, sumComCount };

			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbakls", rows, colfams, quals, values);

			

			rows = new String[] { key3, key3, key3, key3, key3, key3, key3 };
			colfams = new String[] { "R", "R", "R", "B", "B", "B", "B" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"name", "url", "Duration", "showtype" };
			values = new String[] { rowkey, key, "ls", MovieName, playurl,
					Duration, album };

			hbase.putRows("videoinfo", rows, colfams, quals, values);
			hbase.putRows("videoinfobak", rows, colfams, quals, values);

			rows = new String[] { key4, key4, key4, key4, key4, key4, key4,
					key4 };
			colfams = new String[] { "R", "R", "R", "R", "C", "C", "C", "C" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"timestamp", "related", "sumplaycount", "comment", "updown" };
			values = new String[] { rowkey, key, "ls", time, GuessMovie,
					PlayCount, comCount, upDown };

			hbase.putRows("videodynamic", rows, colfams, quals, values);
			hbase.putRows("videodynamicbakls", rows, colfams, quals, values);
			infoDataToMysql(rowkey,time,now);

		} else if (flag == 2) {
			rows = new String[] { key2, key2, key2, key2, key2, key2 };
			colfams = new String[] { "R", "R", "R", "C", "C", "C" };
			quals = new String[] { "inforowkey", "website", "timestamp",
					"score", "sumplaycount", "comment" };
			values = new String[] { rowkey, "ls", time, Grade, sumPlayCount,
					sumComCount };

			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbakls", rows, colfams, quals, values);

			rows = new String[] { key4, key4, key4, key4, key4, key4, key4,
					key4 };
			colfams = new String[] { "R", "R", "R", "R", "C", "C", "C", "C" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"timestamp", "related", "sumplaycount", "comment", "updown" };
			values = new String[] { rowkey, key, "ls", time, GuessMovie,
					PlayCount, comCount, upDown };

			hbase.putRows("videodynamic", rows, colfams, quals, values);
			hbase.putRows("videodynamicbakls", rows, colfams, quals, values);
			dataToMysql(rowkey,time,now);
		}

		rows = null;
		colfams = null;
		quals = null;
		values = null;
		details = null;
		rowkey = null;

		MovieName = null;// 电影或者电视剧名称
		MovieYear = null;// 年份
		MovieCountry = null;// 出产地
		MovieDirector = null;// 导演
		MovieActor = null;// 演员
		MovieType = null;// 类型
		Intro = null;// 简介
		PlayCount = null;// 播放量
		sumPlayCount = null;// 总播放量
		comCount = null;// 评论数
		Grade = null;// 评分
		sumComCount = null;// 总评论数
		GuessMovie = null;// 猜你喜欢
		key = null;
		Duration = null;// 时长
		PictureURL = null;// 图片url
		Name = null;// 剧集名
		Episode = null;// 集数
		album = null;
	}

	public void infoDataToMysql(String rowkey,String time,String date) {
		ArrayList<TextValue> movieInfoValues = new ArrayList<TextValue>();
		ArrayList<TextValue> videoInfoValues = new ArrayList<TextValue>();
		ArrayList<TextValue> movieDynamicValues = new ArrayList<TextValue>();
		ArrayList<TextValue> videoDynamicValues = new ArrayList<TextValue>();
		
		int yearInt=-1;

		int minute=-1;
		if(!Duration.isEmpty()&&Duration!="无"){
			Duration=Duration.substring(0,Duration.lastIndexOf(":"));
			int hour=Duration.indexOf(":");
			if(hour>0){
				minute=Integer.valueOf(Duration.substring(0,hour));
				minute=minute*60+Integer.valueOf(Duration.substring(hour));
			}
			else{
				minute=Integer.valueOf(Duration);
			}
		}
		
		if(MovieYear.contains("-")){
			MovieYear = MovieYear.substring(0, 4);
			yearInt = Integer.valueOf(MovieYear);
		}
		if(yearInt==0){
			yearInt=-1;
		}
		
		TextValue categoryValue = new TextValue();
		categoryValue.text = "category";
		categoryValue.value = "tv";
		movieInfoValues.add(categoryValue);
		movieDynamicValues.add(categoryValue);
		
		TextValue rowkeyValue = new TextValue();
		rowkeyValue.text = "rowkey";
		rowkeyValue.value = rowkey.trim()+"+ls";
		movieInfoValues.add(rowkeyValue);
		movieDynamicValues.add(rowkeyValue);
		videoInfoValues.add(rowkeyValue);
		videoDynamicValues.add(rowkeyValue);
		
		TextValue infoRowkeyValue = new TextValue();
		infoRowkeyValue.text = "inforowkey";
		infoRowkeyValue.value = rowkey.trim()+"+ls";
		videoInfoValues.add(infoRowkeyValue);
		videoDynamicValues.add(infoRowkeyValue);
		
		TextValue areaValue = new TextValue();
		areaValue.text = "area";
		areaValue.value = MovieCountry;
		movieInfoValues.add(areaValue);
		
		TextValue durationValue = new TextValue();
		durationValue.text = "duration";
		durationValue.value = minute;
		movieInfoValues.add(durationValue);
		
		TextValue directorValue = new TextValue();
		directorValue.text = "director1";
		directorValue.value = MovieDirector;
		movieInfoValues.add(directorValue);
		
		TextValue yearValue = new TextValue();
		yearValue.text = "year";
		yearValue.value = yearInt;
		movieInfoValues.add(yearValue);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date());
		sd.replace("-", "");
		TextValue crawlTime = new TextValue();
		crawlTime.text = "crawltime";
		crawlTime.value = sd;
		movieInfoValues.add(crawlTime);
		videoInfoValues.add(crawlTime);
		
		TextValue timeValue = new TextValue();
		timeValue.text = "time";
		timeValue.value = "-1";
		movieInfoValues.add(timeValue);
		
		TextValue ytimeValue = new TextValue();
		ytimeValue.text = "ytime";
		ytimeValue.value = "-1";
		movieInfoValues.add(ytimeValue);
		
		TextValue summarValue = new TextValue();
		summarValue.text = "summarize";
		summarValue.value = Intro;
		movieInfoValues.add(summarValue);
		
		TextValue priceValue = new TextValue();
		priceValue.text = "price";
		priceValue.value = "-1";
		movieInfoValues.add(priceValue);
		
		TextValue nameValue = new TextValue();
		nameValue.text = "moviename";
		nameValue.value = Name;
		movieInfoValues.add(nameValue);
		
		TextValue website = new TextValue();
		website.text = "website";
		website.value = "ls";
		movieInfoValues.add(website);
		movieDynamicValues.add(website);
		videoInfoValues.add(website);
		videoDynamicValues.add(website);
		
		
		String[] typeSplits = MovieType.split("@");
		String[] actorSplits = MovieActor.split("@");
		String temp = "";
		for (int i = 0; i < actorSplits.length - 1; i++) {
			TextValue actortv = new TextValue();
			actortv.text = "mainactor" + (i + 1);
			temp = actorSplits[i + 1];
			if (temp.contains("$"))
				temp = temp.substring(0, temp.indexOf("$"));
			actortv.value = temp;
			movieInfoValues.add(actortv);
			if (i == 2)
				break;
		}
		for (int i = 0; i < typeSplits.length - 1; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i + 1);
			typetv.value = typeSplits[i + 1];
			movieInfoValues.add(typetv);
			if (i == 2)
				break;
		}
		
		TextValue sumPlayCountValue = new TextValue();
		sumPlayCountValue.text = "sumPlayCount";
		sumPlayCountValue.value = sumPlayCount;
		movieDynamicValues.add(sumPlayCountValue);
		
		TextValue manValue = new TextValue();
		manValue.text = "man";
		manValue.value = "-1";
		movieDynamicValues.add(manValue);
		
		TextValue womenValue = new TextValue();
		womenValue.text = "women";
		womenValue.value = "-1";
		movieDynamicValues.add(womenValue);
		
		TextValue flagValue = new TextValue();
		flagValue.text = "flag";
		flagValue.value = 1;// n 2:y
		movieDynamicValues.add(flagValue);
		videoDynamicValues.add(flagValue);
		
		TextValue sumCommentValue = new TextValue();
		sumCommentValue.text = "comment";
		sumCommentValue.value = sumComCount;
		movieDynamicValues.add(sumCommentValue);
		
		TextValue commentValue = new TextValue();
		commentValue.text = "comment";
		commentValue.value = comCount;
		videoDynamicValues.add(commentValue);
		
		TextValue freeValue = new TextValue();
		freeValue.text = "free";
		freeValue.value = "1";
		movieDynamicValues.add(freeValue);
		
		TextValue showtypeValue = new TextValue();
		showtypeValue.text = "showtype";
		showtypeValue.value = album;
		videoInfoValues.add(showtypeValue);
		
		TextValue MovieNameValue = new TextValue();
		MovieNameValue.text = "name";
		MovieNameValue.value = MovieName;
		videoInfoValues.add(MovieNameValue);
		
		TextValue playRowkeyValue = new TextValue();
		playRowkeyValue.text = "playrowkey";
		playRowkeyValue.value = key;
		videoInfoValues.add(playRowkeyValue);
		videoDynamicValues.add(playRowkeyValue);
		
		TextValue timestampValue = new TextValue();
		timestampValue.text = "timestamp";
		timestampValue.value = time.substring(0,10);
		videoDynamicValues.add(timestampValue);
		movieDynamicValues.add(timestampValue);
		
		TextValue collectValue = new TextValue();
		collectValue.text = "collect";
		collectValue.value = "-1";
		videoDynamicValues.add(collectValue);

		TextValue outsideValue = new TextValue();
		outsideValue.text = "outside";
		outsideValue.value = "-1";
		videoDynamicValues.add(outsideValue);
		
		TextValue upValue = new TextValue();
		upValue.text = "up";
		upValue.value = up;
		videoDynamicValues.add(upValue);
		movieDynamicValues.add(upValue);
		
		TextValue downValue = new TextValue();
		downValue.text = "down";
		downValue.value = down;
		videoDynamicValues.add(downValue);
		movieDynamicValues.add(downValue);
		
		TextValue playcountValue = new TextValue();
		playcountValue.text = "sumplaycount";
		playcountValue.value = PlayCount;
		videoDynamicValues.add(playcountValue);
		
		TextValue scoreValue = new TextValue();
		scoreValue.text = "score";
		scoreValue.value = Grade;
		movieDynamicValues.add(scoreValue);
		
		if(!existMoviedynamic(rowkey+"+ls",date)){
			jdbconn.insert(movieDynamicValues, "moviedynamic"+date);
		}
		if(!existMovieinfo(rowkey+"+ls")){
			jdbconn.insert(movieInfoValues, "movieinfo");
		}
		jdbconn.insert(videoDynamicValues, "videodynamic"+date);
		jdbconn.insert(videoInfoValues, "videoinfo");
	}

	public void dataToMysql(String rowkey, String time,String date) {
		ArrayList<TextValue> movieDynamicValues = new ArrayList<TextValue>();
		ArrayList<TextValue> videoDynamicValues = new ArrayList<TextValue>();

		TextValue categoryValue = new TextValue();
		categoryValue.text = "category";
		categoryValue.value = "tv";
		movieDynamicValues.add(categoryValue);
		
		TextValue rowkeyValue = new TextValue();
		rowkeyValue.text = "rowkey";
		rowkeyValue.value = rowkey.trim()+"+ls";
		movieDynamicValues.add(rowkeyValue);
		videoDynamicValues.add(rowkeyValue);
		
		TextValue infoRowkeyValue = new TextValue();
		infoRowkeyValue.text = "inforowkey";
		infoRowkeyValue.value = rowkey.trim();
		videoDynamicValues.add(infoRowkeyValue);
		
		TextValue website = new TextValue();
		website.text = "website";
		website.value = "ls";
		movieDynamicValues.add(website);
		videoDynamicValues.add(website);
		
		TextValue sumPlayCountValue = new TextValue();
		sumPlayCountValue.text = "sumPlayCount";
		sumPlayCountValue.value = sumPlayCount;
		movieDynamicValues.add(sumPlayCountValue);
		
		TextValue manValue = new TextValue();
		manValue.text = "man";
		manValue.value = "-1";
		movieDynamicValues.add(manValue);
		
		TextValue womenValue = new TextValue();
		womenValue.text = "women";
		womenValue.value = "-1";
		movieDynamicValues.add(womenValue);
		
		TextValue flagValue = new TextValue();
		flagValue.text = "flag";
		flagValue.value = 1;
		movieDynamicValues.add(flagValue);
		videoDynamicValues.add(flagValue);
		
		TextValue sumCommentValue = new TextValue();
		sumCommentValue.text = "comment";
		sumCommentValue.value = sumComCount;
		movieDynamicValues.add(sumCommentValue);
		
		TextValue commentValue = new TextValue();
		commentValue.text = "comment";
		commentValue.value = comCount;
		videoDynamicValues.add(commentValue);
		
		TextValue playRowkeyValue = new TextValue();
		playRowkeyValue.text = "playrowkey";
		playRowkeyValue.value = key;
		videoDynamicValues.add(playRowkeyValue);
		
		TextValue freeValue = new TextValue();
		freeValue.text = "free";
		freeValue.value = "1";
		movieDynamicValues.add(freeValue);
		
		TextValue timestampValue = new TextValue();
		timestampValue.text = "timestamp";
		timestampValue.value = time.substring(0,10);
		videoDynamicValues.add(timestampValue);
		movieDynamicValues.add(timestampValue);
		
		TextValue collectValue = new TextValue();
		collectValue.text = "collect";
		collectValue.value = "-1";
		videoDynamicValues.add(collectValue);

		TextValue outsideValue = new TextValue();
		outsideValue.text = "outside";
		outsideValue.value = "-1";
		videoDynamicValues.add(outsideValue);
		
		TextValue scoreValue = new TextValue();
		scoreValue.text = "score";
		scoreValue.value = Grade;
		movieDynamicValues.add(scoreValue);
		
		TextValue upValue = new TextValue();
		upValue.text = "up";
		upValue.value = up;
		videoDynamicValues.add(upValue);

		TextValue downValue = new TextValue();
		downValue.text = "down";
		downValue.value = down;
		videoDynamicValues.add(downValue);
		
		TextValue playcountValue = new TextValue();
		playcountValue.text = "sumplaycount";
		playcountValue.value = PlayCount;
		videoDynamicValues.add(playcountValue);
		
		if(!existMoviedynamic(rowkey+"+ls",date)){
			jdbconn.insert(movieDynamicValues, "moviedynamic"+date);
		}
		jdbconn.insert(videoDynamicValues, "videodynamic"+date);
	}
	
	public boolean existMovieinfo(String rowkey){
		int count=-1;
		count=jdbconn.executeQueryCount("select count(*) as count from movieinfo where rowkey=\'"+rowkey+"\'");
		
		if(count>0){
			return true;
		}
		else{
			return false;
		}
	}
	
	public boolean existMoviedynamic(String rowkey,String date){
		int count=-1;
		count=jdbconn.executeQueryCount("select count(*) as count  from moviedynamic"+date+" where rowkey=\'"+rowkey+"\'");
		if(count>0){
			return true;
		}
		else{
			return false;
		}
	}
	
	public String Type(String details, int type) {
		StringBuilder MovieType = new StringBuilder();
		int typeli = details.indexOf("class=", type);
		if (type < 0 || typeli < 0)
			return "无";
		else
			details = details.substring(type, typeli);
		while (type > 0) {
			int typeBegin = details.indexOf("_blank\">");
			int typeEnd = details.indexOf("</a>", typeBegin);
			MovieType.append("@");
			MovieType.append(details.substring(typeBegin + 8, typeEnd));
			details = details.substring(typeEnd);
			type = details.indexOf("_blank\">");
		}
		return MovieType.toString();
	}

	public String YouLike(String details,String date) {
		StringBuilder GuessMovie = new StringBuilder();
		int pidBeg = details.indexOf("pid\"");
		if (pidBeg < 0)
			return "无";
		while (pidBeg > 0) {
			int pidEnd = details.indexOf(",", pidBeg + 8);
			if (pidEnd < 5 || pidBeg < 0)
				break;
			else {
				ArrayList<TextValue> reference = new ArrayList<TextValue>();
				String rowkey=details.substring(pidBeg + 7, pidEnd);
				TextValue rowkeyRefer = new TextValue();
				rowkeyRefer.text = "rowkey";
				rowkeyRefer.value = rowkey;
				reference.add(rowkeyRefer);
				
				TextValue refer = new TextValue();
				refer.text = "reference";
				refer.value = "1";
				reference.add(refer);
				
				TextValue website = new TextValue();
				website.text = "website";
				website.value = "ls";
				reference.add(website);
				jdbconn.insert(reference, "reference"+date);
				GuessMovie.append("@");
				GuessMovie.append(rowkey);
			}
			details = details.substring(pidEnd);
			pidBeg = details.indexOf("pid\"");
		}
		return GuessMovie.toString();
	}

	public String DirectorScore(String details, String MovieDirector) {
		int DriectorGradeBegin = details.indexOf("director\"");
		if (DriectorGradeBegin > 0) {
			int DriectorGradeEnd = details.indexOf(",", DriectorGradeBegin);
			if (DriectorGradeEnd > 15)
				MovieDirector += "$"
						+ details.substring(DriectorGradeBegin + 15,
								DriectorGradeEnd);
		}
		return MovieDirector;
	}

	public String ActorScore(String details) {
		StringBuilder MovieType = new StringBuilder();
		int actorBeg = details.indexOf("主演");
		int actorEn = details.indexOf("class=", actorBeg);
		if (actorBeg < 0 || actorEn < 0)
			return "";
		else
			details = details.substring(actorBeg, actorEn);

		while (actorBeg > 0) {
			int typeBegin = details.indexOf("_blank\">");
			int typeEnd = details.indexOf("</a>", typeBegin);
			MovieType.append("@");
			MovieType.append(details.substring(typeBegin + 8, typeEnd));
			details = details.substring(typeEnd);
			actorBeg = details.indexOf("_blank\">");
		}
		return MovieType.toString();
	}

}
