package ParserData.YoukuParserData;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import hbase.HBaseCRUD;

public class DongmanParser {

	public HBaseCRUD hbase;
	public JDBCConnection conn;

	public DongmanParser(HBaseCRUD hbase,JDBCConnection conn) {
		this.hbase = hbase;
		this.conn = conn;
	}
	public boolean exist(String text,String value,String table){
		int count=0;
		count=conn.executeQueryCount("select count(*) from " + table + " where " + text + " = '"+value+"'");
		if(count > 0)
			return true;
		else return false;
	}
	public void playParser(String source, String key, String infoRowKey,
			String url, String date, String type) throws Exception{
		if (source == null || source.length() < 20) {
			//add by hanjiaxing 
			conn.log("hanjiangxue", key, 1, "yk", "", "dongman video source null", 2);
			return;
		}
		
		String mykey = key;
		String details = source.toString();
		String name = "";
		String showtype = "";
		String othername = "";
		String playcount = "";
		String comment = "";
		int showtypebegin = details.indexOf("showtype");
		if(showtypebegin >= 0) {
			int end = details.indexOf("\"",showtypebegin+10);
			if(end >= 0 && end - showtypebegin < 15)
				showtype = details.substring(showtypebegin + 10,end);
		}
		int subtitlebegin = details.indexOf("meta name=\"irTitle\"");
		if(subtitlebegin >= 0) {
			int subtitleend = details.indexOf("\" />",subtitlebegin + 29);
			if(subtitleend >= 0)
				othername = details.substring(subtitlebegin + 29, subtitleend);
		}
		
		String Dramavideo = "";
		String upVideoTimes = "";
		String downVideoTimes = "";
		
		int indexbeginSum = details.indexOf("\"vv\":");
		if(indexbeginSum > 0) {
			int indexendSum = details.indexOf(",",indexbeginSum);
			if(indexendSum > indexbeginSum) {
				playcount = details.substring(indexbeginSum + 5,indexendSum);
			}
		}
		if(playcount.equals(""))
			System.out.println(details.substring(indexbeginSum));
		try{
			int i = Integer.parseInt(playcount);
		} catch(Exception e) {
			e.printStackTrace();
		}
		if (details.indexOf("*@@@*") > 0) {
			String dynamic = source.substring(source.indexOf("*@@@*") + 5);
			String str = dynamic;
			if (dynamic != null) {
				int dingcaiIndex = str.indexOf("<li><label>顶 / 踩");
				if(dingcaiIndex > 0) {
					int indexEnd = str.indexOf("</span>",dingcaiIndex);
					String dingcai = str.substring(dingcaiIndex + 43,indexEnd);
					String[] dingcais = dingcai.split("/");
					if(dingcais.length == 2) {
						upVideoTimes = dingcais[0].trim().replaceAll(",", "");
						downVideoTimes = dingcais[1].trim().replaceAll(",", "");
					}
				}
				int index = str.indexOf("totalComment2");
				if (index >= 0) {
					int endindex = str.indexOf("<", index);
					if (index + 27 <= endindex)
						comment = str.substring(index + 27, endindex).replaceAll(",", "");
				}
			}
		}
		String recommend = "";
		int indexrec = details.indexOf("*$$$*");
		if (indexrec >= 0) {
			recommend = details.substring(indexrec + 5);
		}
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		int index = mykey.indexOf("ptth");
		String urlkey = mykey.substring(0,index+4);
		String timestampkey = mykey.substring(index+4);
		String key1 = infoRowKey + "+" + urlkey + "+" + "yk";
		String key2 = infoRowKey + "+" + urlkey  + "+" + "yk" + "+" + "n"+ "+" + timestampkey;
		rows = new String[] {key1,key1,key1,key1,key1,key1};
		colfams = new String[] { "R", "R", "R", "B", "B","B" };
		quals = new String[] { "inforowkey", "playrowkey", "website",
				"name","url","showtype" };
		values = new String[] { infoRowKey, urlkey, "yk", othername,url,showtype };
		try {
			hbase.putRows("videoinfo2", rows, colfams, quals, values);
			hbase.putRows("videoinfobak2", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		rows = new String[] {key2,key2,key2,key2,key2,key2,key2,key2,key2,key2};
		colfams = new String[] { "R", "R", "R", "R", "R","C","C","C","C","C"};
		quals = new String[] { "inforowkey", "playrowkey", "website","flag","timestamp","recommend","related","updown","sumplaycount","comment" };
		values = new String[] { infoRowKey, urlkey, "yk","n", timestampkey,recommend,Dramavideo,upVideoTimes+"@"+ downVideoTimes,playcount,comment};
		try {
			hbase.putRows("videodynamic2", rows, colfams, quals, values);
			hbase.putRows("videodynamicbak2", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void infoParser(String source, String key, String url,
			String date, String type) throws Exception{
		if (source == null || source.length() < 20) {
			//add by hanjiaxing 
			conn.log("hanjiangxue", key, 1, "yk", "", "dongman movie source null", 2);
			return;
		}
		String mykey = key;
		String moviename = "";
		String movieyear = "";
		String jpgurl = "";
		String score = "";
		String douscore = "";
		String dingcai = "";
		String alias = "";
		String Time = "";
		String YTime = "";
		String Lan = "";
		String Are = "";
		String Type = "";
		String Director = "";
		String Actor = "";
		String sum = "";
		String shou = "";
		String comment = "";
		String incresum = "";
		String dura = "";  //jijun bofang
		String price = "";
		String detail = "";
		String award = "";
		String Actorprod = "";
		String sourcerela = "";
		String totalNum = ""; // jishu
		
		int free = 1;
		if(source.indexOf("<em>免费试看</em>") > 0)
			free = 0;
		int indexname = source.indexOf("<span class=\"name\">");
		if (indexname > 0) {
			String sourcename = source.substring(indexname);
			int indexna = sourcename.indexOf("class=\"name\">");
			int endname = sourcename.indexOf("</span>");
			moviename = sourcename.substring(+indexna + 13, endname);
			// System.out.println(moviename);
		}
		int indexyear = source.indexOf("<span class=\"pub\">");
		if (indexyear > 0) {
			String conyear = source.substring(indexyear);
			int indexye = conyear.indexOf("class=\"pub\">");
			int endyear = conyear.indexOf("</span>");
			movieyear = conyear.substring(indexye + 12, endyear);
			// System.out.println(movieyear);
		}
		int indexjpg = source.indexOf("<li class=\"thumb\"><img src='");
		if (indexjpg > 0) {
			int endjpg = source.indexOf("alt=", indexjpg);
			jpgurl = source.substring(indexjpg + 28, endjpg - 2);
			// System.out.println(jpgurl);
		}
		int indexsc = source.indexOf("观众过少");
		if (indexsc >= 0) {
			score = "观众过少，评分积累中";
		} else {
			int indexscore = source.indexOf("<em class=\"num\">");
			if (indexscore > 0) {
				String sourcescore = source.substring(indexscore);
				int indexsco = sourcescore.indexOf("num\">");
				int endscore = sourcescore.indexOf("</em></span>");
				if(indexsco >= 0 && endscore >= 0 && endscore > indexsco)
					score = sourcescore.substring(indexsco + 5, endscore);
			}
		}
		int indexdou = source.indexOf("<label>豆瓣:</label>");
		if (indexdou > 0) {
			int indexd = source.indexOf("<label>豆瓣:</label>");
			String scocontent = source.substring(indexd);
			int indexscor = scocontent.indexOf("</label>");
			int endscore = scocontent.indexOf("</span>");
			douscore = scocontent.substring(indexscor + 8, endscore);
			// System.out.println(douscore);

		}
		if (indexsc >= 0) {
			dingcai = "观众过少，无人顶踩";
		} else {
			int indexdingcai = source.indexOf("span class=\"rating\" title=\"");
			if (indexdingcai > 0) {
				int enddingcai = source
						.indexOf(">",indexdingcai);
				if (enddingcai > 0)
					dingcai = source.substring(indexdingcai + 27, enddingcai);
			}
		}
		int indexali = source.indexOf("<label>别名:</label>");
		if (indexali > 0) {
			String sourceali = source.substring(indexali);
			int indexalia = sourceali.indexOf("别名:</label>");
			int endalias = sourceali.indexOf("</li>");
			if(indexalia >= 0 && endalias >= indexalia + 11) {
				String aliase = sourceali.substring(indexalia + 11, endalias);
				alias = aliase.trim();
			}
			// System.out.println(alias);
		}
		int indextime = source
				.indexOf("<span class=\"pub\"><label>上映:</label>");
		if (indextime > 0) {
			String sourcetime = source.substring(indextime);
			int indexT = sourcetime.indexOf("</label>");
			int endT = sourcetime.indexOf("</span>");
			Time = sourcetime.substring(indexT + 8, endT);
			// System.out.println(Time);
		}
		int indexytime = source.indexOf("<label>优酷上映:</label>");
		if (indexytime > 0) {
			String sourceytime = source.substring(indexytime);
			int indexYT = sourceytime.indexOf("</label>");
			int endYT = sourceytime.indexOf("</span>");
			YTime = sourceytime.substring(indexYT + 8, endYT);
			// System.out.println(YTime);
		}
		int indexlan = source.indexOf("<label>语言:</label>");
		if (indexlan > 0) {
			String sourcelan = source.substring(indexlan);
			int indexL = sourcelan.indexOf("</label>");
			int endL = sourcelan.indexOf("</span>");
			if(indexL >= 0 && endL > indexL + 8)
				Lan = sourcelan.substring(indexL + 8, endL);
			// System.out.println(Lan);
		}
		int indexarea = source.indexOf("<label>地区:</label>");
		if (indexarea > 0) {
			String contmp = source.substring(indexarea);
			int indexar = contmp.indexOf("<label>地区:</label>");
			int endAre = contmp.indexOf("</span>");
			if (indexar >= 0 && endAre >= 0 && endAre > indexar) {
			String sourceare = contmp.substring(indexar, endAre); // sourceare是国家。
			String con = "http://www.youku.com/v_olist/c_100_a_";
			String endcon = ".html";
			int indexa = 0;
			int endindexa = 0;
			StringBuffer Area = new StringBuffer();
			while (indexa >= 0) {
				indexa = sourceare.indexOf(con, endindexa);
				if (indexa == -1) {
					break;
				}
				endindexa = sourceare.indexOf(endcon, indexa);

				String Ar = sourceare.substring(indexa + 36, endindexa);

				Area.append(Ar + "@");
			}
			Are = Area.toString();
			}
			// System.out.println(Are);
		}

		int indextype = source.indexOf("<label>类型:</label>");
		if (indextype > 0) {
			String contmp = source.substring(indextype);
			int indexar = contmp.indexOf("<label>类型:</label>");
			int endAre = contmp.indexOf("</span>");
			if(indexar >= 0 && endAre >= 0 && endAre > indexar) {
			String sourceare = contmp.substring(indexar, endAre); // sourceare是国家。
			String con = "http://www.youku.com/v_olist/c_100_g_";
			String endcon = ".html";
			int indexa = 0;
			int endindexa = 0;
			StringBuffer Typ = new StringBuffer();
			while (indexa >= 0) {
				indexa = sourceare.indexOf(con, endindexa);
				if (indexa == -1) {
					break;
				}
				endindexa = sourceare.indexOf(endcon, indexa);
				String Ty = sourceare.substring(indexa + 36, endindexa);
				Typ.append(Ty + "@");
			}
			Type = Typ.toString();
			// System.out.println(Type);
			}
		}

		int indexdir = source.indexOf("<label>导演:</label>");
		if (indexdir > 0) {
			String contmp1 = source.substring(indexdir);
			int indexar1 = contmp1.indexOf("<label>导演:</label>");
			int endAre1 = contmp1.indexOf("</span>");
			if (indexar1 >= 0 && endAre1 >= 0) {
				String sourceare1 = contmp1.substring(indexar1, endAre1); // sourceare1是导演。
				String con1 = "target=\"_blank\">";
				String endcon1 = "</a>";
				int indexa1 = 0;
				int endindexa1 = 0;
				StringBuffer Direct = new StringBuffer();
				while (indexa1 >= 0) {
					indexa1 = sourceare1.indexOf(con1, endindexa1);
					if (indexa1 == -1) {
						break;
					}
					endindexa1 = sourceare1.indexOf(endcon1, indexa1);
					String Dir = sourceare1.substring(indexa1 + 16, endindexa1);
					Direct.append(Dir + "@");

				}
				Director = Direct.toString();
				// System.out.println(Director);
			}
		}
		int indexact = source.indexOf("<label>声优:</label>");
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<label>声优:</label>");
			int endAre1 = contmp1.indexOf("</span>");
			if(indexar1 >= 0 && endAre1 >=0) {
			String sourceare1 = contmp1.substring(indexar1, endAre1); // sourceare1是主演。
			String con1 = "target=\"_blank\">";
			String endcon1 = "</a>";
			int indexa1 = 0;
			int endindexa1 = 0;
			StringBuffer Acto = new StringBuffer();
			while (indexa1 >= 0) {
				indexa1 = sourceare1.indexOf(con1, endindexa1);
				if (indexa1 == -1) {
					break;
				}
				endindexa1 = sourceare1.indexOf(endcon1, indexa1);
				String Act = sourceare1.substring(indexa1 + 16, endindexa1);
				Acto.append(Act + "@");

			}
			Actor = Acto.toString();
			// System.out.println(Actor);
			}
		}
		int indexsum = source.indexOf("<label>总播放:</label>");
		if (indexsum > 0) {
			String consum = source.substring(indexsum);
			int insum = consum.indexOf("</label>");
			int endsum = consum.indexOf("</span>");
			if (insum >= 0 && endsum >= 0)
				sum = consum.substring(insum + 8, endsum);
			// System.out.println("总播放数："+sum);
		}

		int indexcom = source.indexOf("<label>评论:</label>");
		if (indexcom > 0) {
			String consum = source.substring(indexcom);
			int insum = consum.indexOf("class=\"num\">");
			int endsum = consum.indexOf("</em>");
			if(endsum >= 0 && insum >= 0)
			comment = consum.substring(insum + 12, endsum);
			// System.out.println("评论："+sum);
		}

		int indexshou = source.indexOf("<label>收藏:</label>");
		if (indexshou > 0) {
			String consum = source.substring(indexshou);
			int insum = consum.indexOf("class=\"num\">");
			int endsum = consum.indexOf("</em>");
			if(endsum >= 0 && insum >= 0)
			shou = consum.substring(insum + 12, endsum);
			// System.out.println("收藏："+sum);
		}
		int indexincr = source.indexOf("<label>今日新增播放:</label>");
		if (indexincr > 0) {
			String consum = source.substring(indexincr);
			int insum = consum.indexOf("</label>");
			int endsum = consum.indexOf("</span>");
			if(endsum >= 0 && insum >= 0)
			incresum = consum.substring(insum + 8, endsum);
			// System.out.println("今日新增播放："+sum);
		}

		int indexdura = source.indexOf("<label>集均播放:</label>");
		if (indexdura > 0) {
			String consum = source.substring(indexdura);
			int insum = consum.indexOf("</label>");
			int endsum = consum.indexOf("</span>");
			if(endsum >= insum && insum>=0)
			dura = consum.substring(insum + 8, endsum);
			// System.out.println("时长："+sum);
		}

		int indextotalNum = source.indexOf("basenotice");
		if (indextotalNum > 0) {
			int index = source.indexOf("<", indextotalNum);
			if(index >= 0)
				totalNum = source.substring(indextotalNum + 12, index).trim();
		}
		int indexprice = source.indexOf("<strong>");
		int endprice = source.indexOf("</strong>");
		if (indexprice > 0 && endprice > 0) {

			price = source.substring(indexprice + 8, endprice);
			// System.out.println("价格："+price);
		}

		int indexde = source
				.indexOf("<span class=\"short\" style=\"display:block;\">");
		if (indexde > 0) {
			String consum = source.substring(indexde);
			int insum = consum
					.indexOf("<span class=\"long\" style=\"display:block;\">");

			int endsum = consum.indexOf("</span>");
			if(insum >= 0 && endsum >= insum+44)
			detail = consum.substring(insum + 44, endsum);
			// System.out.println("概况："+sum);
		}

		int indexawd = source.indexOf("<div class=\"awardymod\" id=\"award\">");
		if (indexawd > 0) {
			int indexaw = source.indexOf("title=\"", indexawd);
			int endaw = source.indexOf("\">", indexaw);
			award = source.substring(indexaw + 7, endaw);
			// System.out.println("获奖:"+award);
		}

		int indexus = source.indexOf("<div class=\"users\">");
		if (indexus > 0) {
			String contmp1 = source.substring(indexus);
			int indexar1 = contmp1.indexOf("<div class=\"users\">");
			int endAre1 = contmp1.indexOf("</div><!--.users-->");
			String sourcepar = "";
			if (indexar1 >= 0 && endAre1 >= 0) {
				sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是演员信息。
				int indexur = 0;
				int endur = 0;

				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<ul", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</ul>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur);// 每个演员的信息。
						int indexactor = actor
								.indexOf("class=\"avatar\"><a  target=\"_blank\"  title=\"");
						int endactora = actor.indexOf("\" charset=\"",
								indexactor);
						String Act = actor
								.substring(indexactor + 43, endactora);
						Acto.append(Act + "$");
						int indexactpro = actor
								.indexOf("class=\"portray\" title=\"");
						if (indexactpro > 0) {
							int endactpro = actor.indexOf("\">", indexactpro);
							String Actpro = actor.substring(indexactpro + 23,
									endactpro);
							Acto.append(Actpro + "@");
						}
					}
				}
				Actorprod = Acto.toString();
				// System.out.println("演职员表："+Actorprod);
			}

		}

		int indexrela = source.indexOf("<div class=\"colllist1s\">");
		if (indexrela > 0) {
			String contmp1 = source.substring(indexrela);
			int indexar1 = contmp1.indexOf("<div class=\"colllist1s\">");
			int endAre1 = contmp1.indexOf("</div>");
			if (indexar1 >= 0 && endAre1 >= 0)
				sourcerela = contmp1.substring(indexar1, endAre1); // sourcepar是演员信息。
			// System.out.println(sourcerela); //相关推荐所有的信息。
		}
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		int index = mykey.indexOf("ptth");
		String urlkey = mykey.substring(0,index+4);
		String timestampkey = mykey.substring(index+4);
		String key1 = urlkey + "+" + "yk";
		String key2 = urlkey + "+" + "yk" + "+" + "n" + "+" + timestampkey;
		
		rows = new String[] { key1, key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B", "B",
				"B", "B", "B", "B", "B", "B", "B", "B", "B",  "B"};
		quals = new String[] { "inforowkey", "year", "website", "name",
				"pictureURL", "othername", "time", "ytime", "lan", "area", "type",
				"director", "mainactor", "actorlist", "price",
				"category", "summarize", "rewards", "url" };
		values = new String[] { urlkey, movieyear, "yk", moviename, jpgurl,alias,Time,YTime,Lan,Are,Type,Director,Actor,Actorprod,price,"dongman",detail,award,url};
		try {
			hbase.putRows("movieinfo2", rows, colfams, quals, values);
			hbase.putRows("movieinfobak2", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		rows = new String[] { key2, key2,key2, key2, key2, key2,key2, key2, key2, key2,key2, key2, key2,key2, key2,key2,key2};
		colfams = new String[] { "R", "R", "R","R", "R", "C", "C", "C", "C", 
				"C", "C", "C", "C","C", "C", "C","C"};
		quals = new String[] { "inforowkey", "year", "website","flag","timestamp", "name",
				"score", "doubanscore", "updown", "sumplaycount","averageplaycount","jishu","collect",
				"comment","todayplaycount" ,"category","free"};
		values = new String[] { urlkey, movieyear, "yk","n",timestampkey, moviename, score,douscore,dingcai,sum,dura,totalNum,shou,comment,incresum,"dongman",free+""};
		try {
			hbase.putRows("moviedynamic2", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak2", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}// infoParser
	
	public boolean existMovieinfo(String rowkey){
		int count=-1;
		count=conn.executeQueryCount("select count(*) from movieinfo where rowkey=\'"+rowkey+"\'");
		return true;
	}
}
