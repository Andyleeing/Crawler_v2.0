package ParserData.Iqiyi;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.TextValue;
import Utils.JDBCConnection;
import hbase.HBaseCRUD;

public class parser {
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
public	String category ="";
public  String showtype = "";
public	String name = "";
public	String type = "";
public  String key="";
public	String time = "";
public	String year = "";
public	String director = "";
public	String summarize = "";
public	String duration = "";
public	String area = "";
public	String lan = "";
public	String actorlist = "";
public String inforowkey="";
public String playrowkey="";
public String tv="";

public String reference="";
public String updown="";
public String score="";
public String comment="";
public String timestamp="";
public String free="";
public String sumplaycount="-1";
long timee;
public String sd;
public static ArrayList movielist = new ArrayList();
public static ArrayList videolist = new ArrayList();
public parser(HBaseCRUD hbase, JDBCConnection jdbconn,String time,String sd) {
		this.jdbconn = jdbconn;
		this.hbase = hbase;
		this.timestamp=time;
		this.sd=sd;
	}

	public static int indexdur = -1;

	public void Movie(String source,JDBCConnection jdbc) {
		indexdur = 0;
		category = "movie";	
		String othername = "";
		String ppre = "";
		String mpre = "";	
		String ycomment = "";
	
		int indexurl = source.indexOf("info------------------");
		int endurl = source.indexOf("<!");
		String url = source.substring(indexurl + 22, endurl);// 当前处理电影的url.
		int indexhtml = url.indexOf(".html");
		String mykey = url;

		int indexname = source.indexOf("<meta name=\"title\" content=\"");
		if (indexname > 0) {
			int endname = source.indexOf("-", indexname);
			// System.out.println(url);
			if (endname > indexname) {
				String sname = source.substring(indexname + 28, endname);
				name = sname.replaceAll("&nbsp;", "");
			}
			// System.out.println(name);
		}

		int indexty = source.indexOf("<meta itemprop=\"genre\" content=\"");
		if (indexty >= 0) {
			int indextype = 0;
			int endtype = 0;
			int n = 1;
			StringBuffer typeu = new StringBuffer();
			while (indextype >= 0) {
				indextype = source.indexOf(
						"<meta itemprop=\"genre\" content=\"", endtype);
				endtype = source.indexOf("\"/>", indextype);
				if (indextype < endtype && indextype >= 0) {
					String sourceype = source
							.substring(indextype + 32, endtype);
					if (n == 1) {
						typeu.append(sourceype);
					} else {
						typeu.append("@" + sourceype);
					}

				}
				n++;
				if (n > 3)
					break;

			}
			type = typeu.toString();
			// System.out.println(type);

		}

		int indexali = source.indexOf("<meta itemprop=\"othername\"", indexty);
		if (indexali > 0) {
			String contali = source.substring(indexali);
			int indexothername = contali.indexOf("content=\"");
			int endothername = contali.indexOf("/>");
			String alia = contali.substring(indexothername + 9, endothername);
			othername = alia.replaceAll("\"", "");
			// System.out.println("英文名：" + othername);
		}
		int indexsco = source.indexOf("<em data-videomark-elem=\"intmark\">",
				indexali);
		int endsco = source.indexOf("</span>", indexsco);
		// System.out.println(url);
		if (endsco > indexsco && indexsco >= 0 && endsco >= 0) {
			String rescore = source.substring(indexsco, endsco).replaceAll(
					"\\D*", "");
			StringBuilder a = new StringBuilder(rescore);
			a.insert(1, ".");
			score = a.toString();
			// System.out.println("分数" + score);
			a = null;
		}

		int indextime = source.indexOf(" <meta itemprop=\"datePublished\"",
				indexname);
		if (indextime > 0) {
			String contali = source.substring(indextime);
			int indextim = contali.indexOf("content=\"");
			int endtime = contali.indexOf("/>");
			if (endtime > indextim && indextim >= 0) {
				String tim = contali.substring(indextim + 9, endtime);
				time = tim.replaceAll("\"", "");
				// System.out.println("上映时间：" + time);
			}
		}
		if (time.length() > 6) {
			year = time.substring(0, 4);
		}

		int indexdiract = source.indexOf(
				"<p id=\"widget-director\" rseat=\"导演\">", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("rseat=\"导演\">");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		indexdiract = source.indexOf("<p id=\"datainfo-director\"", indexty);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-director\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1 && endAre1 > indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		int indexdescrip = source
				.indexOf(" <meta itemprop=\"description\" content=\"",indexname);
		if (indexdescrip > -1) {
			String indescri = source.substring(indexdescrip);
			int indexdes = indescri.indexOf("content=\"");
			int enddes = indescri.indexOf("/>");
			if (enddes > indexdes && indexdes >= 0) {
				String indestmp = indescri.substring(indexdes + 9, enddes);
				summarize = indestmp.replaceAll("\"", "");
				// System.out.println(summarize);// 描述。
			}
		}

		String ttime = null;// 后面取顶踩数时需要。
		indexdur = source.indexOf("<meta itemprop=\"duration\"", indexdur + 1);
		if (indexdur >= 0) {
			String indescri = source.substring(indexdur);
			int indexdes = indescri.indexOf("content=\"");
			int enddes = indescri.indexOf("/>");
			if (enddes > indexdes && indexdes >= 0) {
				String indestmp = indescri.substring(indexdes + 9, enddes);
				ttime = indestmp.replaceAll("\\D*", "");
			}
			if (ttime.length() > 1) { // "";
				int second = Integer.parseInt(ttime);
				int duratio = second / 60;
				duration = Integer.toString(duratio);
				// System.out.println("时长：" + duration + "分钟");
			}
		}

		int indexloc = source
				.indexOf("<meta itemprop=\"contentLocation\" content=\"",indexty);
		if (indexloc > -1) {
			String indescri = source.substring(indexloc);
			int indexdes = indescri.indexOf("content=\"");
			int enddes = indescri.indexOf("/>");
			if (indexdes < enddes && indexdes >= 0) {
				String indestmp = indescri.substring(indexdes + 9, enddes);
				area = indestmp.replaceAll("\"", "");
				// System.out.println(area);
			}
		}

		int indexlan = source
				.indexOf("<meta itemprop=\"inLanguage\" content=\"",indexty);
		if (indexlan > -1) {
			String indescri = source.substring(indexlan);
			int indexdes = indescri.indexOf("content=\"");
			int enddes = indescri.indexOf("/>");
			if (enddes > indexdes && indexdes >= 0) {
				String indestmp = indescri.substring(indexdes + 9, enddes);
				lan = indestmp.replaceAll("\"", "");
				// System.out.println(lan);
			}
		}

		int indexact = source.indexOf(
				"<div class=\"peos-info\" id=\"widget-actor\">", indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("id=\"widget-actor\">");
			int endAre1 = contmp1.indexOf("</div>", indexar1);
			if (endAre1 == -1) {
				endAre1 = contmp1.indexOf("Precentge", indexar1);
			}
			String sourcepar = contmp1.substring(indexar1 + 18, endAre1); // sourcepar是导演信息。
			int indexur = 0;
			int endur = 0;
			int n = 1;
			StringBuffer Actjue = new StringBuffer();
			while (indexur >= 0) {
				indexur = sourcepar.indexOf("<p>", endur);
				endur = sourcepar.indexOf("</p>", indexur);// endur已经是最后一个是需要检测indexur.
				if (indexur >= 0) {
					String actor = sourcepar.substring(indexur, endur + 4);// 每个演员的信息。
					int indexactpro = actor.indexOf("rseat=\"");
					if (indexactpro >= 0) {
						int endactpro = actor.indexOf("\">", indexactpro);
						if (endactpro > indexactpro) {
							String Actpro = actor.substring(indexactpro + 7,
									endactpro);// 每个演员的名字.
							if (n == 1) {
								Actjue.append(Actpro);
							} else {
								Actjue.append("@" + Actpro);
							}
						}
					}
					int indexjue = actor
							.indexOf(">饰</span><span class=\"ml5 fs12 c-666\" rseat=\"");
					if (indexjue >= 0) {
						int endjue = actor.indexOf("\">", indexjue);
						if (endjue > indexjue) {
							String Actju = actor.substring(indexjue + 45,
									endjue);
							String jue = Actju.replaceAll("角色_", "饰");
							Actjue.append("$" + jue);
						}
					}
					int indexactor = actor.indexOf("<a href=\"");
					if (indexactor > -1) {
						int endactora = actor.indexOf("\"", indexactor + 9);
						if (endactora > indexactor) {
							String Act = actor.substring(indexactor + 9,
									endactora); // 每个演员的连接.
							Actjue.append("$" + Act);
						}
					}
					n++;
				}
			}
			actorlist = Actjue.toString();
			// System.out.println("演员表" + actorlist);
		}

		indexdiract = source.indexOf("<p id=\"datainfo-actor\"", indexdiract);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-actor\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1 && endAre1 > indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("主演：" + actorlist);
			}
		}

		int indexsumm = source.indexOf("try{window.Q.__callbacks__.cbgt6rz7(",
				indexdiract);
		if (indexsumm >= 0) {
			String sumplaycounttmp = "";
			int indexsum = source.indexOf(":", indexsumm);
			int end = source.indexOf("}", indexsum);
			if (end > indexsum) {
				sumplaycounttmp = source.substring(indexsum + 1, end);
			}

			if (sumplaycounttmp.contains("万")) {
				double sumplaycountint = Double.parseDouble(sumplaycounttmp
						.replaceAll("万", "")) * 10000;
				BigDecimal big = new BigDecimal(sumplaycountint);
				sumplaycount = big.toString().replaceAll("\\.0", "");
			} else if (sumplaycounttmp.contains("亿")) {
				Long sumplaycountint = Math
						.round(Double.parseDouble(sumplaycounttmp.replaceAll(
								"亿", "")) * 100000000);
				BigDecimal big = new BigDecimal(sumplaycountint);
				sumplaycount = big.toString().replaceAll("\\.0", "");
			} else {
				sumplaycount = sumplaycounttmp;
			}

			// System.out.println("总播放数" + sumplaycount);
		}
		if(sumplaycount=="-1") {
		jdbc.log("李辉", url+"+iy", 1, "iy", url, "播放量没有抓到", 2);
		return;
		}

		String upcount = null;
		String downcount = null;
		int indexup = source.indexOf("duration\":" + ttime + ",\"upCount\":",
				indexact);
		if (indexup > -1) {
			int endup = source.indexOf(",", indexup + ttime.length() + 20);
			if (endup > indexup) {
				upcount = source
						.substring(indexup + ttime.length() + 21, endup);
				// System.out.println("顶：" + upcount);
			}
		}
		int indexdown = source.indexOf("downCount\":", indexup);
		if (indexdown > -1) {
			int enddown = source.indexOf(",", indexdown);
			if (enddown > indexdown) {
				downcount = source.substring(indexdown + 11, enddown);
				// System.out.println("踩：" + downcount);
			}
		}

		if (upcount != null && downcount != null) {
			updown = upcount + "@" + downcount;
			// System.out.println(updown);
		}

		int commindex = source.indexOf("Comment", indexsumm);
		if (commindex >= 0) {
			int coudex = source.indexOf("\"count\":", commindex);
			int enddex = source.indexOf(",", coudex);
			// // System.out.println(url);
			if (enddex > coudex && coudex >= 0) {
				comment = source.substring(coudex + 8, enddex);
				// // System.out.println(consu);
				if (comment.length() > 12) {
					comment = "0";
				}
			}
		}

		int ycommindex = source.indexOf("Ycomment", commindex);
		if (ycommindex >= 0) {
			int coudex = source.indexOf("\"count\":", ycommindex);
			int enddex = source.indexOf(",", coudex);
			// // System.out.println(url);
			if (enddex > coudex && coudex >= 0) {
				ycomment = source.substring(coudex + 8, enddex);
				// // System.out.println(consu);
				if (ycomment.length() > 12) {
					ycomment = "0";
				}
			}
		}

		int indexpre = source.indexOf("playCountPCMobileCb", indexsumm);
		if (indexpre > -1) {
			int indexp = source.indexOf("p\":", indexpre);
			int endp = source.indexOf(",", indexp);
			if (endp > indexp) {
				ppre = source.substring(indexp + 3, endp);
				// System.out.println("PC占比：" + ppre + "%");
			}
			int indexm = source.indexOf("m\":", endp);
			int endm = source.indexOf("}", indexm);
			if (endm > indexm) {
				mpre = source.substring(indexm + 3, endm);
				// System.out.println("移动占比：" + mpre + "%");
			}
		}

		int index = source.indexOf("try{window.Q.__callbacks__.", indexpre);
		int end = source.indexOf("})", index);
		if (end > index && index >= 0) {
			String cont = source.substring(index, end);
			int indexx = 0;
			int endd = 0;
			int n = 1;

			StringBuffer urlt = new StringBuffer();
			while (indexx >= 0) {
				indexx = cont.indexOf("\"vid\"", endd);

				if (indexx >= 0) {
					int iindexurl = cont.indexOf("url\":\"", indexx);
					int eendurl = cont.indexOf("\",", iindexurl);
					endd = eendurl;
					String ttmpurl = cont.substring(iindexurl + 6, eendurl);
					if (n == 1)
						urlt.append(ttmpurl);
					else
						urlt.append("@@" + ttmpurl);
					n++;
					if (n > 10)
						break;
				}
			}
			reference = urlt.toString();
			// System.out.println(reference);

		}

		int freeindex = source.indexOf("var __getPayBtn__cb_={\"data\":{",
				index);
		if (freeindex >= 0) {
			free = "0";
		} else {
			free = "1";
		}

		String key1 = mykey + "+" + "iy";
		String key2 = mykey + "+" + "iy" + "+" + timee;
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		rows = new String[] { key1, key1, key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1, key1, key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B", "B",
				"B", "B", "B", "B", "B", "C" };
		quals = new String[] { "inforowkey", "website", "year", "category",
				"name", "type", "othername", "time", "lan", "area", "director",
				"actorlist", "duration", "summarize", "crawltime" };
		values = new String[] { mykey, "iy", year, category, name, type,
				othername, time, lan, area, director, actorlist, duration,
				summarize, IqiyiParse.crawltime };
		try {
			hbase.putRows("movieinfoiy", rows, colfams, quals, values);
			hbase.putRows("movieinfobakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2,
				key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "B", "C", "C", "C", "C", "C",
				"C", "C", "C", "C", "C" };
		quals = new String[] { "inforowkey", "website", "year", "category",
				"name", "updown", "sumplaycount", "ppre", "mpre", "comment",
				"ycomment", "score", "reference", "free" };
		values = new String[] { mykey, "iy", "year", category, name, updown,
				sumplaycount, ppre, mpre, comment, ycomment, score, reference,
				free };
		try {
			hbase.putRows("moviedynamiciy", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (infoIsExist(key1, "movie") == 0) { // 说明movieinfo不存在该记录，所以导入到movieinfo;
			Importmovieinfo(key1);
		}
		try {
			Importmoviedynamic(key1, category);
		} catch (Exception e) {
			e.printStackTrace();
		}

		rows = null;
		colfams = null;
		quals = null;
		values = null;
		source = null;
		name = null;
		type = null;
		othername = null;
		time = null;
		director = null;
		summarize = null;
		duration = null;
		area = null;
		lan = null;
		actorlist = null;
		updown = null;
		category = null;
		sumplaycount = null;
		ppre = null;
		mpre = null;
		score = null;
		reference = null;
		free = null;
		comment = null;
		ycomment = null;
	}

	public void MoviePh(String source,JDBCConnection jdbc) {
	
		 showtype = "预告片";
		String othername = "";
		String ppre = "";
		String mpre = "";
		String guess = "";
		
		String ycomment = "";
		int idstart = source.indexOf("albumId");
		String idcode = source.substring(idstart);
		int idst = idcode.indexOf("albumId");
		int ided = idcode.indexOf(",");
		String did = idcode.substring(idst, ided);
		String id = did.replaceAll("\\D*", ""); // 得到ＩＤ号。
		int indexurl = source.indexOf("info------------------");
		int endurl = source.indexOf("<!");
		String urla = source.substring(indexurl + 22, endurl);// 当前处理电影的url.

		String mykey = urla;

		int indexsta = urla.indexOf("http://");
		int indexena = urla.indexOf(".html");
		String url = urla.substring(indexsta, indexena + 5);
		int indexname = source.indexOf("name=\"irAlbumName\" content=\"");
		if (indexname >= 0) {
			String conte = source.substring(indexname);
			int indexna = conte.indexOf("content=\"");
			int endna = conte.indexOf("\"", indexna + 10);
			name = conte.substring(indexna + 9, endna);
			// System.out.println(name);
			int indexsumm = source.indexOf(
					"try{window.Q.__callbacks__.cbgt6rz7(", indexname);
			if (indexsumm >= 0) {
				String sumplaycounttmp = "";
				int indexsum = source.indexOf(":", indexsumm);
				int end = source.indexOf("}", indexsum);
				if (end > indexsum) {
					sumplaycounttmp = source.substring(indexsum + 1, end);
				}
				if (sumplaycounttmp.length() < 25) {
					if (sumplaycounttmp.contains("万")) {
						double sumplaycountint = Double
								.parseDouble(sumplaycounttmp
										.replaceAll("万", "")) * 10000;
						BigDecimal big = new BigDecimal(sumplaycountint);
						sumplaycount = big.toString().replaceAll("\\.0", "");
					} else if (sumplaycounttmp.contains("亿")) {
						Long sumplaycountint = Math.round(Double
								.parseDouble(sumplaycounttmp
										.replaceAll("亿", "")) * 100000000);
						BigDecimal big = new BigDecimal(sumplaycountint);
						sumplaycount = big.toString().replaceAll("\\.0", "");
					} else {
						sumplaycount = sumplaycounttmp;
					}

					// System.out.println("总播放数" + sumplaycount);
				}
				
			}
			if(sumplaycount==null) {
				jdbc.log("李辉", url+"+iy", 1, "iy", url, "未能得到播放量", 2);
				return;
			}
			int commindex = source.indexOf("Comment", indexsumm);
			if (commindex >= 0) {
				int coudex = source.indexOf("\"count\":", commindex);
				int enddex = source.indexOf(",", coudex);
				// // System.out.println(url);
				comment = source.substring(coudex + 8, enddex);
				// // System.out.println(consu);
				if (comment.length() > 12) {
					comment = "0";
				}
			}

			int ycommindex = source.indexOf("Ycomment", commindex);
			if (ycommindex >= 0) {
				int coudex = source.indexOf("\"count\":", ycommindex);
				int enddex = source.indexOf(",", coudex);
				// // System.out.println(url);
				ycomment = source.substring(coudex + 8, enddex);
				// // System.out.println(consu);
				if (ycomment.length() > 12) {
					ycomment = "0";
				}
			}

			int indexty = source.indexOf("id=\"widget-videotag\">", indexname);
			if (indexty > 0) {
				String contypee = source.substring(indexty);
				int indetypee = contypee.indexOf("id=\"widget-videotag\">");
				int endyear = contypee.indexOf("</span>", indetypee);
				// // System.out.println(indetypee);
				// // System.out.println(endyear);
				if (indetypee >= 0 && endyear >= 0) {
					String sourceype = contypee.substring(indetypee + 22,
							endyear);
					int indextype = 0;
					int endtype = 0;
					StringBuffer typeu = new StringBuffer();
					int n = 1;
					while (indextype >= 0) {
						indextype = sourceype.indexOf("class=\"green\">",
								endtype); // endur已经是最后一个是需要检测indexur.
						endtype = sourceype.indexOf("</a>", indextype);
						if (indextype >= 0) {
							String typee = sourceype.substring(indextype + 14,
									endtype);
							if (n == 1) {
								typeu.append(typee);
							} else {
								typeu.append("@" + typee);
							}
							n++;

						}
						if (n > 3)
							break;
					}
					type = typeu.toString();
					// System.out.println(type);
				}
			}

			int indexali = source.indexOf("<meta itemprop=\"othername\"",
					indexname);
			if (indexali > 0) {
				String contali = source.substring(indexali);
				int indexothername = contali.indexOf("content=\"");
				int endothername = contali.indexOf("/>");
				String alia = contali.substring(indexothername + 9,
						endothername);
				othername = alia.replaceAll("\"", "");

				// System.out.println("英文名：" + othername);
			}

			int indextime = source.indexOf(
					"itemprop=\"datePublished\" content=\"", indexname);
			if (indextime > 0) {
				String contali = source.substring(indextime);
				int indextim = contali.indexOf("content=\"");
				int endtime = contali.indexOf("\"", indextim + 10);
				String tim = contali.substring(indextim + 9, endtime);
				time = tim.replaceAll("\"", "");

				// System.out.println("上映时间：" + time);
			}
			if (time.length() > 5) {
				year = time.substring(0, 4);
			}

			int indexdiract = source.indexOf(
					"<p id=\"widget-director\" rseat=\"导演\">", indexty);
			if (indexdiract > 0) {
				String contmp1 = source.substring(indexdiract);
				int indexar1 = contmp1.indexOf("rseat=\"导演\">");
				int endAre1 = contmp1.indexOf("</p>");
				if (indexar1 >= 0 && endAre1 >= 0) {
					String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = sourcepar.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String actor = sourcepar.substring(indexur,
									endur + 4);// 每个导演的信息。
							int indexactor = actor.indexOf("\">");
							int endactora = actor.indexOf("</a>", indexactor);
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
							int indexactpro = actor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = actor.indexOf("\"",
										indexactpro + 10);
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
						n++;
					}
					director = Acto.toString();

					// System.out.println("导演：" + director);
				}
			}

			indexdiract = source.indexOf("<p id=\"datainfo-director\"",
					indexname);
			if (indexdiract >= 0) {
				String contmp1 = source.substring(indexdiract);
				int indexar1 = contmp1.indexOf("<p id=\"datainfo-director\"");
				int endAre1 = contmp1.indexOf("</p>");
				if (endAre1 > -1 && endAre1 > indexar1) {
					String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = sourcepar.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String actor = sourcepar.substring(indexur,
									endur + 4);// 每个导演的信息。
							int indexactor = actor.indexOf("\">");
							int endactora = actor.indexOf("</a>", indexactor);
							if (endactora > indexactor && indexactor >= 0) {
								String Act = actor.substring(indexactor + 2,
										endactora); // 每个导演的名字。
								if (n == 1) {
									Acto.append(Act);
								} else {
									Acto.append("@" + Act);
								}
							}
							int indexactpro = actor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = actor.indexOf("\"",
										indexactpro + 10);
								if (endactpro > indexactpro) {
									String Actpro = actor.substring(
											indexactpro + 9, endactpro);// 每个导演的URL.
									Acto.append("$" + Actpro);
								}
							}
						}
						n++;
					}
					director = Acto.toString();
					// System.out.println("导演：" + director);
				}
			}

			int indexdescrip = source
					.indexOf("<meta itemprop=\"description\" content=\"");
			if (indexdescrip > -1) {
				String indescri = source.substring(indexdescrip);
				int indexdes = indescri.indexOf("content=\"");
				int enddes = indescri.indexOf("/>");
				if (enddes > indexdes && indexdes >= 0) {
					String indestmp = indescri.substring(indexdes + 9, enddes);
					summarize = indestmp.replaceAll("\"", "");
					// System.out.println(summarize);// 描述。
				}
			}

			String Time = null;// 后面取顶踩数时需要。
			int indexdura = 0;
			if (indexdur > 0) {
				indexdura = source.indexOf("<meta itemprop=\"duration\"",
						indexdur + 10);
			} else {
				indexdura = source.indexOf("<meta itemprop=\"duration\"");
			}
			if (indexdura > -1) {
				String indescri = source.substring(indexdura);
				int indexdes = indescri.indexOf("content=\"");
				int enddes = indescri.indexOf("/>", indexdes);
				if (indexdes >= 0 && enddes >= 0) {
					String indestmp = indescri.substring(indexdes + 9, enddes);
					Time = indestmp.replaceAll("\\D*", "");
					int second = Integer.parseInt(Time);
					int duratio = second / 60;
					int Second = second % 60;
					duration = Integer.toString(duratio);
					// System.out.println("时长：" + duration + "分" + Second + "秒");
				}
			}

			int indexloc = source
					.indexOf("<meta itemprop=\"contentLocation\" content=\"",indexdescrip);
			if (indexloc > -1) {
				String indescri = source.substring(indexloc);
				int indexdes = indescri.indexOf("content=\"");
				int enddes = indescri.indexOf("/>");
				if (indexdes < enddes && indexdes >= 0) {
					String indestmp = indescri.substring(indexdes + 9, enddes);
					area = indestmp.replaceAll("\"", "");
					// System.out.println(area);
				}
			}

			int indexlan = source
					.indexOf("<meta itemprop=\"inLanguage\" content=\"",indexdescrip);
			if (indexlan > -1) {
				String indescri = source.substring(indexlan);
				int indexdes = indescri.indexOf("content=\"");
				int enddes = indescri.indexOf("/>");
				if (enddes > indexdes && indexdes >= 0) {
					String indestmp = indescri.substring(indexdes + 9, enddes);
					lan = indestmp.replaceAll("\"", "");
					// System.out.println(lan);
				}
			}

			int indexact = source.indexOf(
					"<div class=\"peos-info\" id=\"widget-actor\">", indexname);
			if (indexact > 0) {
				String contmp1 = source.substring(indexact);
				int indexar1 = contmp1.indexOf("id=\"widget-actor\">");
				int endAre1 = contmp1.indexOf("</div>");
				if (indexar1 >= 0 && endAre1 >= 0) {
					String sourcepar = contmp1
							.substring(indexar1 + 18, endAre1); // sourcepar是导演信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Actjue = new StringBuffer();
					while (indexur >= 0) {
						indexur = sourcepar.indexOf("<p>", endur);
						endur = sourcepar.indexOf("</p>", indexur);// endur已经是最后一个是需要检测indexur.
						if (indexur >= 0) {
							String actor = sourcepar.substring(indexur,
									endur + 4);// 每个演员的信息。
							int indexactpro = actor.indexOf("rseat=\"");
							if (indexactpro >= 0) {
								int endactpro = actor.indexOf("\">",
										indexactpro);
								String Actpro = actor.substring(
										indexactpro + 7, endactpro);// 每个演员的名字.
								if (n == 1) {
									Actjue.append(Actpro);
								} else {
									Actjue.append("@" + Actpro);
								}
							}
							int indexjue = actor
									.indexOf(">饰</span><span class=\"ml5 fs12 c-666\" rseat=\"");
							if (indexjue >= 0) {
								int endjue = actor.indexOf("\">", indexjue);
								String Actju = actor.substring(indexjue + 45,
										endjue);
								String jue = Actju.replaceAll("角色_", "饰");
								Actjue.append("$" + jue);
							}
							int indexactor = actor.indexOf("<a href=\"");
							if (indexactor > -1) {
								int endactora = actor.indexOf("\"",
										indexactor + 9);
								String Act = actor.substring(indexactor + 9,
										endactora); // 每个演员的连接.
								Actjue.append("$" + Act);
							}
							n++;
						}
					}
					actorlist = Actjue.toString();

					// System.out.println("演员表" + actorlist);
				}
			}

			indexdiract = source.indexOf("<p id=\"datainfo-actor\"",indexdiract);
			if (indexdiract >= 0) {
				String contmp1 = source.substring(indexdiract);
				int indexar1 = contmp1.indexOf("<p id=\"datainfo-actor\"");
				int endAre1 = contmp1.indexOf("</p>");
				if (endAre1 > -1 && endAre1 > indexar1) {
					String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = sourcepar.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String actor = sourcepar.substring(indexur,
									endur + 4);// 每个导演的信息。
							int indexactor = actor.indexOf("\">");
							int endactora = actor.indexOf("</a>", indexactor);
							if (endactora > indexactor && indexactor >= 0) {
								String Act = actor.substring(indexactor + 2,
										endactora); // 每个导演的名字。
								if (n == 1) {
									Acto.append(Act);
								} else {
									Acto.append("@" + Act);
								}
							}
							int indexactpro = actor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = actor.indexOf("\"",
										indexactpro + 10);
								if (endactpro > indexactpro) {
									String Actpro = actor.substring(
											indexactpro + 9, endactpro);// 每个导演的URL.
									Acto.append("$" + Actpro);
								}
							}
						}
						n++;
					}
					actorlist = Acto.toString();
					// System.out.println("主演：" + actorlist);
				}
			}

			int indexpre = source.indexOf("playCountPCMobileCb", commindex);
			if (indexpre > -1) {
				int indexp = source.indexOf("p\":", indexpre);
				int endp = source.indexOf(",", indexp);
				ppre = source.substring(indexp + 3, endp);

				// System.out.println("PC占比：" + ppre + "%");
				int indexm = source.indexOf("m\":", endp);
				int endm = source.indexOf("}", indexm);
				mpre = source.substring(indexm + 3, endm);

				// System.out.println("移动占比：" + mpre + "%");
			}

			int index = source.indexOf("try{window.Q.__callbacks__.", indexpre);
			int end = source.indexOf("})", index);
			if (end > index && index >= 0) {
				String cont = source.substring(index, end);
				int indexx = 0;
				int endd = 0;
				int n = 1;

				StringBuffer urlt = new StringBuffer();
				while (indexx >= 0) {
					indexx = cont.indexOf("\"vid\"", endd);

					if (indexx >= 0) {
						int iindexurl = cont.indexOf("url\":\"", indexx);
						int eendurl = cont.indexOf("\",", iindexurl);
						endd = eendurl;
						String ttmpurl = cont.substring(iindexurl + 6, eendurl);
						if (n == 1)
							urlt.append(ttmpurl);
						else
							urlt.append("@@" + ttmpurl);
						n++;
						if (n > 10)
							break;
					}
				}
				reference = urlt.toString();
				// System.out.println(reference);

			}

			int upscost = source.indexOf("try{null", index);
			if (upscost >= 0) {
				int upst = source.indexOf("up\":", upscost);
				int upend = source.indexOf("}", upst);
				String up = source.substring(upst + 4, upend);
				int downst = source.indexOf("down\":", upscost);
				int downend = source.indexOf(",", downst);
				String down = source.substring(downst + 6, downend);
				updown = up + "@" + down;
				// System.out.println(updown);

			}

			String key1 = mykey + "+" + urla + "+iy";
			String key2 = mykey + "+" + urla + "+iy" + "+" + timee;
			String[] rows = null;
			String[] colfams = null;
			String[] quals = null;
			String[] values = null;
			inforowkey=mykey;
			playrowkey=mykey;
			rows = new String[] { key1, key1, key1, key1, key1, key1, key1,
					key1, key1, key1, key1, key1, key1, key1, key1, key1, key1,
					key1 };
			colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B",
					"B", "B", "B", "B", "B", "B", "B", "B", "B", "C" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"showtype", "name", "type", "othername", "time", "lan",
					"area", "type", "director", "actorlist", "duration",
					"summarize", "guess", "reference", "crawltime" };
			values = new String[] { mykey, urla, "iy", showtype, name, type,
					othername, time, lan, area, type, director, actorlist,
					duration, summarize, guess, reference, IqiyiParse.crawltime };
			try {
				hbase.putRows("videoinfoiy", rows, colfams, quals, values);
				hbase.putRows("videoinfobakiy", rows, colfams, quals, values);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			rows = new String[] { key2, key2, key2, key2, key2, key2, key2,
					key2, key2, key2, key2 };
			colfams = new String[] { "R", "R", "R", "B", "C", "C", "C", "C",
					"C", "C", "C" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"showtype", "name", "updown", "sumplaycount", "ppre",
					"mpre", "comment", "ycomment" };
			values = new String[] { mykey, urla, "iy", showtype, name, updown,
					sumplaycount, ppre, mpre, comment, ycomment };
			try {
				hbase.putRows("videodynamiciy", rows, colfams, quals, values);
				hbase.putRows("videodynamicbakiy", rows, colfams, quals, values);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (infoIsExist(inforowkey+"+iy", "video") == 0) { // 说明vodeoinfo不存在该记录，所以导入到videoinfo;
				   Importvideoinfo(inforowkey+"+iy");
				}
				try {
					Importvideodynamic(key1);
				} catch (Exception e) {
					e.printStackTrace();
				}
			rows = null;
			colfams = null;
			quals = null;
			values = null;
			source = null;
			showtype = null;
			name = null;
			type = null;
			othername = null;
			time = null;
			director = null;
			summarize = null;
			duration = null;
			area = null;
			lan = null;
			actorlist = null;
			updown = null;
			sumplaycount = null;
			ppre = null;
			mpre = null;
			guess = null;
			reference = null;
			comment = null;
			ycomment = null;
		}
	}

	public void TvInfo(String source,JDBCConnection jdbc) {
	
		 category = "tv";	
		String ppre = "";
		String mpre = "";	
		String ycomment = "";
		String url = "";
		int indexurl = source.indexOf("info------------------");
		if (indexurl >= 0) {
			int endurl = source.indexOf("<!");
			url = source.substring(indexurl + 22, endurl);// 当前处理的电视剧的url.
		}

		String mykey = url;

		int indexname = source.indexOf("<title>");
		if (indexname > 0) { // 一般电视剧或者动漫的提取。
			int endname = source.indexOf("-", indexname);
			String sname = source.substring(indexname + 7, endname);
			name = sname.replaceAll("&nbsp;", "");
			// System.out.println(name);
		}
		if (name.equals("")) {
			int startname = source.indexOf("<h1");
			int endname = source.indexOf("</h1>");
			String nameelse = source.substring(startname + 3, endname);
			int starname = nameelse.lastIndexOf("\">");
			int enname = nameelse.lastIndexOf("</a>");
			name = nameelse.substring(starname + 2, enname);
		}

		int indexht=source.indexOf("</html>");
		
		int ycommindex = source.indexOf("Ycomment", indexht);
		if (ycommindex >= 0) {
			int coudex = source.indexOf("\"count\":", ycommindex);
			int enddex = source.indexOf(",", coudex);
			ycomment = source.substring(coudex + 8, enddex);
			if (ycomment.length() > 12) {
				ycomment = "0";
			}
		}

		int indexyear = source.indexOf("issueTime\">", indexname);
		if (indexyear > 0) {
			int endname = source.indexOf("</a>", indexyear);
			year = source.substring(indexyear + 11, endname);
			// System.out.println(year);
		}

		int indexsum = source.indexOf("id=\"widget-playcount\">", indexname);
		if (indexsum > 0) {
			int endname = source.indexOf("</i>", indexsum);
			String sumplaycounttmp = source.substring(indexsum + 22, endname);
			if (sumplaycounttmp.contains("万")) {
				double sumplaycountint = Double.parseDouble(sumplaycounttmp
						.replaceAll("万", "")) * 10000;
				BigDecimal big = new BigDecimal(sumplaycountint);
				sumplaycount = big.toString().replaceAll("\\.0", "");
			} else if (sumplaycounttmp.contains("亿")) {
				Long sumplaycountint = Math
						.round(Double.parseDouble(sumplaycounttmp.replaceAll(
								"亿", "")) * 100000000);
				BigDecimal big = new BigDecimal(sumplaycountint);
				sumplaycount = big.toString().replaceAll("\\.0", "");
			} else {
				sumplaycount = sumplaycounttmp;
			}

			// System.out.println("总播放：" + sumplaycount);
		}

		if (sumplaycount==null) {
			int indexsumm = source.indexOf(
					"try{window.Q.__callbacks__.cbgt6rz7(", indexht);
			if (indexsumm >= 0) {
				String sumplaycounttmp = "";
				int indexs = source.indexOf(":", indexsumm);
				int end = source.indexOf("}", indexs);
				if (end > indexs) {
					sumplaycounttmp = source.substring(indexs + 1, end);
				}

				if (sumplaycounttmp.contains("万")) {
					double sumplaycountint = Double.parseDouble(sumplaycounttmp
							.replaceAll("万", "")) * 10000;
					BigDecimal big = new BigDecimal(sumplaycountint);
					sumplaycount = big.toString().replaceAll("\\.0", "");
				} else if (sumplaycounttmp.contains("亿")) {
					Long sumplaycountint = Math
							.round(Double.parseDouble(sumplaycounttmp
									.replaceAll("亿", "")) * 100000000);
					BigDecimal big = new BigDecimal(sumplaycountint);
					sumplaycount = big.toString().replaceAll("\\.0", "");
				} else {
					sumplaycount = sumplaycounttmp;
				}

				// System.out.println("总播放数" + sumplaycount);
			}
		}
		if(sumplaycount==null) {
			jdbc.log("李辉", url+"+iy", 1, "iy", url, "播放量没有抓到", 2);
			return;
			}
		

		int indexsc = source.indexOf("try{window.Q.__callbacks__.cbhdqq1s(",
				indexht);
		if (indexsc >= 0) {
			int upst = source.indexOf("up\":", indexsc);
			int upend = source.indexOf("}", upst);
			String up = source.substring(upst + 4, upend);
			int downst = source.indexOf("down\":", indexsc);
			int downend = source.indexOf(",", downst);
			String down = source.substring(downst + 6, downend);
			updown = up + "@" + down;
			// System.out.println(updown);
			int scost = source.indexOf("score\":", indexsc);
			int scoend = source.indexOf(",", scost);
			score = source.substring(scost + 7, scoend);

		}

		int indexarea = source.indexOf("<p class=\"li-mini\">地区：", indexname);
		if (indexarea >= 0) {
			int endty = source.indexOf("</p>", indexarea);
			if (endty >= indexarea) {
				String typee = source.substring(indexarea, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				area = typeu.toString();
				// System.out.println(area);
			}
		}

		indexarea = source.indexOf("<em>类型", indexname);
		if (indexarea >= 0) {
			int indexend = source.indexOf("</em>", indexarea);
			if (indexend > indexarea) {
				String tmparea = source.substring(indexarea, indexend);
				StringBuffer areau = new StringBuffer();
				int index = 0;
				int end = 0;
				int n = 1;
				while (index >= 0) {
					index = tmparea.indexOf("\">", end);
					if (index == -1) {
						break;
					}
					end = tmparea.indexOf("</a>", index);
					String tmpa = tmparea.substring(index + 2, end);
					if (n == 1) {
						areau.append(tmpa);
					} else {
						areau.append("@" + tmpa);
					}
					n++;
					if (n > 3)
						break;
				}
				type = areau.toString();
			}
		}

		indexarea = source.indexOf("<p class=\"li-large\">地区：", indexname);
		if (indexarea >= 0) {
			int endty = source.indexOf("</p>", indexarea);
			if (endty >= indexarea) {
				String typee = source.substring(indexarea, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				area = typeu.toString();
				// System.out.println(area);
			}
		}

		int indexlan = source.indexOf("<p class=\"li-mini\">语言：", indexname);
		if (indexlan >= 0) {
			int endty = source.indexOf("</p>", indexlan);
			if (endty >= indexlan) {
				String typee = source.substring(indexlan, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				lan = typeu.toString();
				// System.out.println(lan);
			}
		}

		indexlan = source.indexOf("<p class=\"li-large\">语言：", indexname);
		if (indexlan >= 0) {
			int endty = source.indexOf("</p>", indexlan);
			if (endty >= 0) {
				String typee = source.substring(indexlan, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				lan = typeu.toString();
				// System.out.println(lan);
			}
		}

		int indexdiract = source.indexOf("<p class=\"li-mini\">导演：", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p class=\"li-mini\">导演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > indexar1 && indexar1 >= 0) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		indexdiract = source.indexOf("<p class=\"li-large\">导演：", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p class=\"li-large\">导演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > indexar1 && indexar1 >= 0) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		indexdiract = source.indexOf("<em>导演：", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<em>导演：");
			int endAre1 = contmp1.indexOf("</em>");
			if (endAre1 > indexar1 && indexar1 >= 0) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}
		indexdiract = source.indexOf("<p id=\"datainfo-director\"", indexname);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-director\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1 && endAre1 > indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		int indexact = source.indexOf("<em>主演：",indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<em>主演：");
			int endAre1 = contmp1.indexOf("</em>");
			if (endAre1 > indexar1 && indexar1 >= 0) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是演员的信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个演员的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("演员表：" + actorlist);
			}
		}

		indexdiract = source.indexOf("<p id=\"datainfo-actor\"", indexdiract);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-actor\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1 && endAre1 > indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("主演：" + actorlist);
			}
		}

		indexact = source.indexOf("<p class=\"li-large\">主演：", indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<p class=\"li-large\">主演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 >= 0 && endAre1 >= indexar1) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是演员的信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个演员的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("演员表：" + actorlist);
			}
		}

		indexarea = source.indexOf("<em>地区：");
		if (indexarea >= 0) {
			int indexend = source.indexOf("</em>", indexarea);
			if (indexend > indexarea) {
				String tmparea = source.substring(indexarea, indexend);
				StringBuffer areau = new StringBuffer();
				int index = 0;
				int end = 0;
				int n = 1;
				while (index >= 0) {
					index = tmparea.indexOf("\">", end);
					if (index == -1) {
						break;
					}
					end = tmparea.indexOf("</a>", index);
					String tmpa = tmparea.substring(index + 2, end);
					if (n == 1) {
						areau.append(tmpa);
					} else {
						areau.append("@" + tmpa);
					}
					n++;
				}
				area = areau.toString();
				// System.out.println(area);
			}
		}

		indexarea = source.indexOf("<em>语言：");
		if (indexarea >= 0) {
			int indexend = source.indexOf("</em>", indexarea);
			if (indexend > indexarea) {
				String tmparea = source.substring(indexarea, indexend);
				StringBuffer areau = new StringBuffer();
				int index = 0;
				int end = 0;
				int n = 1;
				while (index >= 0) {
					index = tmparea.indexOf("\">", end);
					if (index == -1) {
						break;
					}
					end = tmparea.indexOf("</a>", index);
					String tmpa = tmparea.substring(index + 2, end);
					if (n == 1) {
						areau.append(tmpa);
					} else {
						areau.append("@" + tmpa);
					}
					n++;
				}
				lan = areau.toString();
			}
		}

		indexact = source.indexOf("<p class=\"li-mini\">主演：", indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<p class=\"li-mini\">主演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 >= indexar1) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是演员的信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个演员的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("演员表：" + actorlist);
			}
		}

		int indexty = source.indexOf("<p class=\"li-large\">类型：", indexname);
		if (indexty >= 0) {
			int endty = source.indexOf("</p>", indexty);
			if (endty > indexty) {
				String typee = source.substring(indexty, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
					if (n > 3)
						break;
				}
				type = typeu.toString();
				// System.out.println(type);
			}
		}
		indexty = source.indexOf("<p class=\"li-mini\">类型：", indexname);
		if (indexty >= 0) {
			int endty = source.indexOf("</p>", indexty);
			if (endty >= indexty) {
				String typee = source.substring(indexty, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
					if (n > 3)
						break;
				}
				type = typeu.toString();
				// System.out.println(type);
			}
		}

		int indexdescrip = source
				.indexOf("<meta name=\"description\" content=\"");
		if (indexdescrip >= 0) {
			int enddescrip = source.indexOf("/>", indexdescrip);
			if (enddescrip >= 0) {
				summarize = source.substring(indexdescrip + 34, enddescrip)
						.replaceAll("\"", "");
				// System.out.println(summarize);
			}
		}

		int indexpre = source.indexOf("playCountPCMobileCb", indexact);
		if (indexpre > -1) {
			int indexp = source.indexOf("p\":", indexpre);
			int endp = source.indexOf(",", indexp);
			if ((endp - indexp) < 10) {
				ppre = source.substring(indexp + 3, endp);
				// System.out.println("PC占比：" + ppre + "%");
				int indexm = source.indexOf("m\":", endp);
				int endm = source.indexOf("}", indexm);
				mpre = source.substring(indexm + 3, endm);
				// System.out.println("移动占比：" + mpre + "%");
			}
		}

		int indexx = source.indexOf("try{window.Q.__callbacks__.cbp5zfwu({",indexht);
		int endd = source.indexOf("})", indexx);
		if (endd > indexx && indexx >= 0) {
			String content = source.substring(indexx, endd);
			int index = 0;
			int end = 0;
			int n = 1;
			String urll = "";
			StringBuffer urla = new StringBuffer();
			while (index >= 0) {
				index = content.indexOf("\"albumUrl\":\"", end);
				if (index >= 0) {
					end = content.indexOf("\",", index);
					if (end > index) {
						urll = content.substring(index + 12, end);
					}

					if (n == 1) {
						urla.append(urll);
					} else {
						urla.append("@@" + urll);
					}
				}

				n++;
			}
			reference = urla.toString();
			// System.out.println(reference);
		}

		int commindex = source.indexOf("Comment", indexx);
		if (commindex >= 0) {
			int coudex = source.indexOf("\"count\":", commindex);
			int enddex = source.indexOf(",", coudex);
			if (enddex > coudex) {
				comment = source.substring(coudex + 8, enddex);
			}
			if (comment.length() > 12) {
				comment = "0";
			}
		}

		String key1 = mykey + "+" + "iy";
		String key2 = mykey + "+" + "iy" + "+" + timee;
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		rows = new String[] { key1, key1, key1, key1, key1, key1, key1, key1,
				key1, key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B", "B",
				"C" };
		quals = new String[] { "inforowkey", "year", "website", "category",
				"name", "director", "type", "actorlist", "summarize",
				"crawltime" };
		values = new String[] { mykey, year, "iy", category, name, director,
				type, actorlist, summarize, IqiyiParse.crawltime };
		try {
			hbase.putRows("movieinfoiy", rows, colfams, quals, values);
			hbase.putRows("movieinfobakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2,
				key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "B", "C", "C", "C", "C", "C",
				"C", "C", "C", "C", "C" };
		quals = new String[] { "inforowkey", "year", "website", "category",
				"name", "score", "sumplaycount", "ppre", "mpre", "comment",
				"ycomment", "updown", "reference", "free" };
		values = new String[] { mykey, year, "iy", category, name, score,
				sumplaycount, ppre, mpre, comment, ycomment, updown, reference,
				free };
		try {
			hbase.putRows("moviedynamiciy", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (infoIsExist(key1, "movie") == 0) { // 说明movieinfo不存在该记录，所以导入到movieinfo;
			Importmovieinfo(key1);
		}
		try {
			Importmoviedynamic(key1, category);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		rows = null;
		colfams = null;
		quals = null;
		values = null;
		source = null;
		category = null;
		name = null;
		year = null;
		sumplaycount = null;
		score = null;
		director = null;
		actorlist = null;
		type = null;
		summarize = null;
		ppre = null;
		lan = null;
		area = null;
		mpre = null;
		reference = null;
		updown = null;
		comment = null;
		ycomment = null;
		free = null;
		System.gc();
	}

	public void DongManInfo(String source,JDBCConnection jdbc) {

		 category = "dongman";	
		String ppre = "";
		String mpre = "";	
		String ycomment = "";
		String url = "";

		int indexurl = source.indexOf("info------------------");
		if (indexurl >= 0) {
			int endurl = source.indexOf("<!");
			url = source.substring(indexurl + 22, endurl);// 当前处理的电视剧的url.
		}

		String mykey = url;
		int indexname = source.indexOf("<title>");
		if (indexname > 0) { // 一般电视剧或者动漫的提取。
			int endname = source.indexOf("-", indexname);
			String sname = source.substring(indexname + 7, endname);
			name = sname.replaceAll("&nbsp;", "");
			// System.out.println(name);
		}
		if (name.equals("")) {
			int startname = source.indexOf("<h1");
			int endname = source.indexOf("</h1>");
			String nameelse = source.substring(startname + 3, endname);
			int starname = nameelse.lastIndexOf("\">");
			int enname = nameelse.lastIndexOf("</a>");
			if (enname > starname && starname >= 0) {
				name = nameelse.substring(starname + 2, enname);
			}
		}
       int indexht=source.indexOf("</html>");
		int ycommindex = source.indexOf("Ycomment", indexht);
		if (ycommindex >= 0) {
			int coudex = source.indexOf("\"count\":", ycommindex);
			int enddex = source.indexOf(",", coudex);
			ycomment = source.substring(coudex + 8, enddex);
			if (ycomment.length() > 12) {
				ycomment = "0";
			}
		}

		int indexyear = source.indexOf("issueTime\">", indexname);
		if (indexyear > 0) {
			int endname = source.indexOf("</a>", indexyear);
			year = source.substring(indexyear + 11, endname);
			// System.out.println(year);
		}
		if (indexyear == -1) {
			int indexy = source.indexOf("<span class=\"sub_title\">");
			if (indexy >= 0) {
				int endy = source.indexOf("</span>", indexy);
				if (endy > indexy)
					year = source.substring(indexy + 24, endy).replaceAll(
							"\\D*", "");
			}
		}

		int indexsum = source.indexOf("id=\"widget-playcount\">", indexname);
		if (indexsum > 0) {
			int endname = source.indexOf("</i>", indexsum);
			String sumplaycounttmp = source.substring(indexsum + 22, endname);
			if (sumplaycounttmp.contains("万")) {
				double sumplaycountint = Double.parseDouble(sumplaycounttmp
						.replaceAll("万", "")) * 10000;
				BigDecimal big = new BigDecimal(sumplaycountint);
				sumplaycount = big.toString().replaceAll("\\.0", "");
			} else if (sumplaycounttmp.contains("亿")) {
				Long sumplaycountint = Math
						.round(Double.parseDouble(sumplaycounttmp.replaceAll(
								"亿", "")) * 100000000);
				BigDecimal big = new BigDecimal(sumplaycountint);
				sumplaycount = big.toString().replaceAll("\\.0", "");
			} else {
				sumplaycount = sumplaycounttmp;
			}

			// System.out.println("总播放：" + sumplaycount);
		}

		if (sumplaycount==null) {
			int indexsumm = source.indexOf(
					"try{window.Q.__callbacks__.cbgt6rz7(", indexht);
			if (indexsumm >= 0) {
				String sumplaycounttmp = "";
				int indexs = source.indexOf(":", indexsumm);
				int end = source.indexOf("}", indexs);
				if (end > indexs) {
					sumplaycounttmp = source.substring(indexs + 1, end);
				}

				if (sumplaycounttmp.contains("万")) {
					double sumplaycountint = Double.parseDouble(sumplaycounttmp
							.replaceAll("万", "")) * 10000;
					BigDecimal big = new BigDecimal(sumplaycountint);
					sumplaycount = big.toString().replaceAll("\\.0", "");
				} else if (sumplaycounttmp.contains("亿")) {
					Long sumplaycountint = Math
							.round(Double.parseDouble(sumplaycounttmp
									.replaceAll("亿", "")) * 100000000);
					BigDecimal big = new BigDecimal(sumplaycountint);
					sumplaycount = big.toString().replaceAll("\\.0", "");
				} else {
					sumplaycount = sumplaycounttmp;
				}

				// System.out.println("总播放数" + sumplaycount);
			}
		}
		
		if(sumplaycount==null) {
			jdbc.log("李辉", url+"+iy", 1, "iy", url, "播放量没有抓到", 2);
			return;
			}

		int indexsc = source.indexOf("try{window.Q.__callbacks__.cbhdqq1s(",
				indexht);
		if (indexsc >= 0) {
			int upst = source.indexOf("up\":", indexsc);
			int upend = source.indexOf("}", upst);
			String up = source.substring(upst + 4, upend);
			int downst = source.indexOf("down\":", indexsc);
			int downend = source.indexOf(",", downst);
			String down = source.substring(downst + 6, downend);
			updown = up + "@" + down;
			// System.out.println(updown);
			int scost = source.indexOf("score\":", indexsc);
			int scoend = source.indexOf(",", scost);
			score = source.substring(scost + 7, scoend);

		}

		int indexarea = source.indexOf("<p class=\"li-mini\">地区：", indexname);
		if (indexarea >= 0) {
			int endty = source.indexOf("</p>", indexarea);
			if (endty >= indexarea) {
				String typee = source.substring(indexarea, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				area = typeu.toString();
				// System.out.println(area);
			}
		}
		indexarea = source.indexOf("<p class=\"li-large\">地区：", indexname);
		if (indexarea >= 0) {
			int endty = source.indexOf("</p>", indexarea);
			if (endty >= indexarea) {
				String typee = source.substring(indexarea, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				area = typeu.toString();
				// System.out.println(area);
			}
		}

		indexarea = source.indexOf("<em>地区：", indexname);
		if (indexarea >= 0) {
			int indexend = source.indexOf("</em>", indexarea);
			if (indexend > indexarea) {
				String tmparea = source.substring(indexarea, indexend);
				StringBuffer areau = new StringBuffer();
				int index = 0;
				int end = 0;
				int n = 1;
				while (index >= 0) {
					index = tmparea.indexOf("\">", end);
					if (index == -1) {
						break;
					}
					end = tmparea.indexOf("</a>", index);
					String tmpa = tmparea.substring(index + 2, end);
					if (n == 1) {
						areau.append(tmpa);
					} else {
						areau.append("@" + tmpa);
					}
					n++;
				}
				area = areau.toString();
			}
		}

		indexarea = source.indexOf("<em>类型", indexname);
		if (indexarea >= 0) {
			int indexend = source.indexOf("</em>", indexarea);
			if (indexend > indexarea) {
				String tmparea = source.substring(indexarea, indexend);
				StringBuffer areau = new StringBuffer();
				int index = 0;
				int end = 0;
				int n = 1;
				while (index >= 0) {
					index = tmparea.indexOf("\">", end);
					if (index == -1) {
						break;
					}
					end = tmparea.indexOf("</a>", index);
					String tmpa = tmparea.substring(index + 2, end);
					if (n == 1) {
						areau.append(tmpa);
					} else {
						areau.append("@" + tmpa);
					}
					n++;
					if (n > 3)
						break;
				}
				type = areau.toString();
			}
		}

		int indexlan = source.indexOf("<p class=\"li-mini\">语言：", indexname);
		if (indexlan >= 0) {
			int endty = source.indexOf("</p>", indexlan);
			if (endty >= indexlan) {
				String typee = source.substring(indexlan, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);

					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				lan = typeu.toString();
				// System.out.println(lan);
			}
		}

		indexlan = source.indexOf("<p class=\"li-large\">语言：", indexname);
		if (indexlan >= 0) {
			int endty = source.indexOf("</p>", indexlan);
			if (endty >= indexlan) {
				String typee = source.substring(indexlan, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);

					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
				}
				lan = typeu.toString();
				// System.out.println(lan);
			}
		}

		indexarea = source.indexOf("<em>语言", indexname);
		if (indexlan >= 0) {
			int indexend = source.indexOf("</em>", indexarea);
			if (indexend > indexarea) {
				String tmparea = source.substring(indexarea, indexend);
				StringBuffer areau = new StringBuffer();
				int index = 0;
				int end = 0;
				int n = 1;
				while (index >= 0) {
					index = tmparea.indexOf("\">", end);
					if (index == -1) {
						break;
					}
					end = tmparea.indexOf("</a>", index);
					String tmpa = tmparea.substring(index + 2, end);
					if (n == 1) {
						areau.append(tmpa);
					} else {
						areau.append("@" + tmpa);
					}
					n++;
				}
				lan = areau.toString();
			}
		}

		int indexdiract = source.indexOf("<p class=\"li-mini\">导演：", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p class=\"li-mini\">导演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (indexar1 > -1 && endAre1 > indexar1) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.

					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}
		indexdiract = source.indexOf("<p class=\"li-large\">导演：", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p class=\"li-large\">导演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (indexar1 >= 0 && endAre1 > indexar1) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		indexdiract = source.indexOf("<em>导演：", indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<em>导演：");
			int endAre1 = contmp1.indexOf("</em>");
			if (indexar1 >= 0 && endAre1 > indexar1) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		indexdiract = source.indexOf("<p id=\"datainfo-director\"", indexname);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-director\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1 && endAre1 > indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				director = Acto.toString();
				// System.out.println("导演：" + director);
			}
		}

		int indexact = source.indexOf("<p class=\"li-large\">主演：", indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<p class=\"li-large\">主演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 >= indexar1 && indexar1 >= 0) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是演员的信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个演员的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("演员表：" + actorlist);
			}
		}

		indexdiract = source.indexOf("<p id=\"datainfo-actor\"", indexdiract);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-actor\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (indexar1 > -1 && endAre1 >= indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("主演：" + actorlist);
			}
		}

		indexact = source.indexOf("<p class=\"li-mini\">主演：", indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<p class=\"li-mini\">主演：");
			int endAre1 = contmp1.indexOf("</p>");
			if (indexar1 >= 0 && endAre1 > indexar1) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是演员的信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个演员的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("演员表：" + actorlist);
			}
		}

		indexact = source.indexOf("<em>主演：", indexname);
		if (indexact > 0) {
			String contmp1 = source.substring(indexact);
			int indexar1 = contmp1.indexOf("<em>主演：");
			int endAre1 = contmp1.indexOf("</em>");
			if (endAre1 > indexar1 && indexar1 >= 0) {
				String contentpar = contmp1.substring(indexar1, endAre1); // contentpar是演员的信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = contentpar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = contentpar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = contentpar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个演员的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				actorlist = Acto.toString();
				// System.out.println("演员表：" + actorlist);
			}
		}

		int indexty = source.indexOf("<p class=\"li-large\">类型：", indexname);
		if (indexty >= 0) {
			int endty = source.indexOf("</p>", indexty + 30);
			if (endty > indexty) {
				String typee = source.substring(indexty, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}
					n++;
					if (n > 3) {
						break;
					}
				}
				type = typeu.toString();
				// System.out.println(type);
			}
		}
		indexty = source.indexOf("<p class=\"li-mini\">类型：");
		if (indexty >= 0) {
			int endty = source.indexOf("</p>", indexty + 30);
			if (endty > indexty) {
				String typee = source.substring(indexty, endty);
				int index = 0;
				int endindex = 0;
				int n = 1;
				StringBuffer typeu = new StringBuffer();
				while (index >= 0) {
					index = typee.indexOf("\">", endindex + 25);
					endindex = typee.indexOf("</a>", index);
					if (endindex > index && index >= 0) {
						String tmp = typee.substring(index + 2, endindex);
						if (n == 1) {
							typeu.append(tmp);
						} else {
							typeu.append("@" + tmp);
						}
					}

					n++;
					if (n > 3)
						break;
				}
				type = typeu.toString();
				typeu = null;
				// System.out.println(type);
			}
		}

		int indexdescrip = source
				.indexOf("<meta name=\"description\" content=\"",indexname);
		if (indexdescrip >= 0) {
			int enddescrip = source.indexOf("/>", indexdescrip);
			if (enddescrip >= 0) {
				summarize = source.substring(indexdescrip + 34, enddescrip)
						.replaceAll("\"", "");
				// System.out.println(summarize);
			}
		}

		int indexpre = source.indexOf("playCountPCMobileCb", indexht);
		if (indexpre > -1) {
			int indexp = source.indexOf("p\":", indexpre);
			int endp = source.indexOf(",", indexp);
			if ((endp - indexp) < 10) {
				ppre = source.substring(indexp + 3, endp);
				// System.out.println("PC占比：" + ppre + "%");
				int indexm = source.indexOf("m\":", endp);
				int endm = source.indexOf("}", indexm);
				mpre = source.substring(indexm + 3, endm);
				// System.out.println("移动占比：" + mpre + "%");
			}
		}

		int index = source.indexOf("try{window.Q.__callbacks__.cbbapijk",
				indexpre);
		int end = source.indexOf("})", index);
		if (end > index && index >= 0) {
			String cont = source.substring(index, end);
			int indexx = 0;
			int endd = 0;
			int n = 1;

			StringBuffer urlt = new StringBuffer();
			while (indexx >= 0) {
				indexx = cont.indexOf("\"vid\"", endd);

				if (indexx >= 0) {
					int iindexurl = cont.indexOf("url\":\"", indexx);
					int eendurl = cont.indexOf("\",", iindexurl);
					endd = eendurl;
					String ttmpurl = cont.substring(iindexurl + 6, eendurl);
					if (n == 1)
						urlt.append(ttmpurl);
					else
						urlt.append("@@" + ttmpurl);
					n++;
					if (n > 10)
						break;
				}
			}
			reference = urlt.toString();
			// System.out.println(reference);

		}

		int commindex = source.indexOf("Comment", index);
		if (commindex >= 0) {
			int coudex = source.indexOf("\"count\":", commindex);
			int enddex = source.indexOf(",", coudex);
			if (enddex > coudex) {
				comment = source.substring(coudex + 8, enddex);
			}
			if (comment.length() > 12) {
				comment = "0";
			}
		}

		String key1 = mykey + "+" + "iy";
		String key2 = mykey + "+" + "iy+" + timee;
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		rows = new String[] { key1, key1, key1, key1, key1, key1, key1, key1,
				key1,key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B", "B",
				"C" };
		quals = new String[] { "inforowkey", "year", "website", "category",
				"name", "director", "type", "actorlist", "summarize",
				"crawltime" };
		values = new String[] { mykey, year, "iy", category, name, director,
				type, actorlist, summarize, IqiyiParse.crawltime };
		try {
			hbase.putRows("movieinfoiy", rows, colfams, quals, values);
			hbase.putRows("movieinfobakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2,
				key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "B", "C", "C", "C", "C", "C",
				"C", "C", "C", "C", "C" };
		quals = new String[] { "inforowkey", "year", "website", "category",
				"name", "score", "sumplaycount", "ppre", "mpre", "comment",
				"ycomment", "updown", "reference", "free" };
		values = new String[] { mykey, year, "iy", category, name, score,
				sumplaycount, ppre, mpre, comment, ycomment, updown, reference,
				free };
		try {
			hbase.putRows("moviedynamiciy", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (infoIsExist(key1, "movie") == 0) { // 说明movieinfo不存在该记录，所以导入到movieinfo;
			Importmovieinfo(key1);
		}
		try {
			Importmoviedynamic(key1, category);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		rows = null;
		colfams = null;
		quals = null;
		values = null;
		source = null;
		category = null;
		name = null;
		updown = null;
		year = null;
		sumplaycount = null;
		score = null;
		director = null;
		actorlist = null;
		type = null;
		lan = null;
		summarize = null;
		ppre = null;
		mpre = null;
		area = null;
		free = null;
		comment = null;
		ycomment = null;
		System.gc();
	}

	public void TvItem(String source) {
	
		String mykey = IqiyiParse.parentId;// 父URL

		String showtype = "正片";
		String url = null;

		int indexurl = source.indexOf("info------------------");
		int endurl = source.indexOf("<!");
		if (indexurl != -1) {
			url = source.substring(indexurl + 22, endurl);// 当前处理的电视剧的url.
		}

		int indexname = source.indexOf("<meta name=\"title\" content=\"");
		if (indexname > 0) {
			int endname = source.indexOf("-", indexname);
			// System.out.println(url);
			if (endname > indexname) {
				String sname = source.substring(indexname + 28, endname);
				name = sname.replaceAll("&nbsp;", "");
			}
			// System.out.println(name);
		}

		int commindex = source.indexOf("Comment", indexname);
		if (commindex >= 0) {
			int coudex = source.indexOf("\"count\":", commindex);
			int enddex = source.indexOf(",", coudex);
			// // System.out.println(url);
			comment = source.substring(coudex + 8, enddex);
			// // System.out.println(consu);
			if (comment.length() > 12) {
				comment = "0";
			}
		}

		int indexx = source.indexOf("try{window.Q.__callbacks__.", commindex);
		int endd = source.indexOf("})", indexx);
		if (endd > indexx && indexx >= 0) {
			String content = source.substring(indexx, endd);
			int index = 0;
			int end = 0;
			int n = 1;
			String urll = "";
			StringBuffer urla = new StringBuffer();
			while (index >= 0) {
				index = content.indexOf("\"albumUrl\":\"", end);
				if (index >= 0) {
					end = content.indexOf("\",", index);
					if (end > index) {
						urll = content.substring(index + 12, end);
					}

					if (n == 1) {
						urla.append(urll);
					} else {
						urla.append("@@" + urll);
					}
				}

				n++;
			}
			reference = urla.toString();
			// System.out.println(reference);
		}

		String up = null;
		String down = null;
		int indexupdown = source.indexOf("upanddown", indexx);
		int indexup = source.indexOf("\"up\":", indexupdown);
		if (indexup >= 0) {
			int endup = source.indexOf(",", indexup);
			if (endup == -1) {
				endup = source.indexOf("}", indexup);
			}
			if (endup > indexup) {
				up = source.substring(indexup + 5, endup);
			}
		}

		int indexdown = source.indexOf("\"down\":", indexupdown);
		if (indexdown >= 0) {
			int enddown = source.indexOf(",", indexdown);
			if (enddown > indexdown) {
				down = source.substring(indexdown + 7, enddown);
			}
		}
		if (up != null || down != null) {
			updown = up + "@" + down;
			// System.out.println(updown);
		}

		String key1 = mykey + " + " + url + "+iy";
		String key2 = mykey + "+" + url + "+iy+" + timee;
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		playrowkey = url;
		inforowkey= mykey;
		rows = new String[] { key1, key1, key1, key1, key1, key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "C" };
		quals = new String[] { "playrowkey", "inforowkey", "website",
				"showtype", "name", "crawltime" };
		values = new String[] { url, mykey, "iy", showtype, name,
				IqiyiParse.crawltime };
		try {
			hbase.putRows("videoinfoiy", rows, colfams, quals, values);
			hbase.putRows("videoinfobakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rows = new String[] { key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "C", "C", "C" };
		quals = new String[] { "playrowkey", "inforowkey", "website", "updown",
				"comment", "reference" };
		values = new String[] { url, mykey, "iy", updown, comment, reference };
		try {
			hbase.putRows("videodynamiciy", rows, colfams, quals, values);
			hbase.putRows("videodynamicbakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (infoIsExist(inforowkey+"+iy", "video") == 0) { // 说明vodeoinfo不存在该记录，所以导入到videoinfo;
			   Importvideoinfo(inforowkey+"+iy");
			}
			try {
				Importvideodynamic(key1);
			} catch (Exception e) {
				e.printStackTrace();
			}

		rows = null;
		colfams = null;
		quals = null;
		values = null;
		source = null;

		url = null;
		name = null;
		updown = null;
		comment = null;
		// guess=null;
		System.gc();
	}

	public void DongManItem(String source) {
	
		String mykey = IqiyiParse.parentId;// 父URL
		 showtype = "正片";
		 String url = "";

		int indexurl = source.indexOf("info------------------");
		int endurl = source.indexOf("<!");
		if (indexurl != -1) {
			url = source.substring(indexurl + 22, endurl);// 当前处理的电视剧的url.
		}

		int indexname = source.indexOf("<meta name=\"title\" content=\"");
		if (indexname > 0) {
			int endname = source.indexOf("-", indexname);
			// System.out.println(url);
			if (endname > indexname) {
				String sname = source.substring(indexname + 28, endname);
				name = sname.replaceAll("&nbsp;", "");
			}
			// System.out.println(name);
		}

		int commindex = source.indexOf("Comment", indexname);
		if (commindex >= 0) {
			int coudex = source.indexOf("\"count\":", commindex);
			int enddex = source.indexOf(",", coudex);
			comment = source.substring(coudex + 8, enddex);
			if (comment.length() > 12) {
				comment = "0";
			}
		}

		int indexx = source.indexOf("try{window.Q.__callbacks__.", commindex);
		int endd = source.indexOf("})", indexx);
		if (endd > indexx && indexx >= 0) {
			String content = source.substring(indexx, endd);
			int index = 0;
			int end = 0;
			int n = 1;
			String urll = "";
			StringBuffer urla = new StringBuffer();
			while (index >= 0) {
				index = content.indexOf("\"albumUrl\":\"", end);
				if (index >= 0) {
					end = content.indexOf("\",", index);
					if (end > index) {
						urll = content.substring(index + 12, end);
					}

					if (n == 1) {
						urla.append(urll);
					} else {
						urla.append("@@" + urll);
					}
				}

				n++;
			}
			reference = urla.toString();
			// System.out.println(reference);
		}
		String up = null;
		String down = null;
		int indexupdown = source.indexOf("upanddown", indexx);
		int indexup = source.indexOf("\"up\":", indexupdown);
		if (indexup >= 0) {
			int endup = source.indexOf(",", indexup);
			if (endup == -1) {
				endup = source.indexOf("}", indexup);
			}
			if (endup > indexup) {
				up = source.substring(indexup + 5, endup);
			}
		}

		int indexdown = source.indexOf("\"down\":", indexupdown);
		if (indexdown >= 0) {
			int enddown = source.indexOf(",", indexdown);
			if (enddown > indexdown) {
				down = source.substring(indexdown + 7, enddown);
			}
		}
		if (up != null || down != null) {
			updown = up + "@" + down;
			// System.out.println(updown);
		}
		inforowkey=mykey;
	    playrowkey=url;
		String key1 = mykey + "+" + url + "+iy";// 父URL+子URL+iy;
		String key2 = mykey + "+" + url + "+iy+" + timee;
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		rows = new String[] { key1, key1, key1, key1, key1, key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "C" };
		quals = new String[] { "playrowkey", "inforowkey", "website",
				"showtype", "name", "crawltime" };
		values = new String[] { url, mykey, "iy", showtype, name,
				IqiyiParse.crawltime };
		try {
			hbase.putRows("videoinfoiy", rows, colfams, quals, values);
			hbase.putRows("videoinfobakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rows = new String[] { key2, key2, key2, key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "C", "C", "C" };
		quals = new String[] { "playrowkey", "inforowkey", "website", "updown",
				"comment", "reference" };
		values = new String[] { url, mykey, "iy", updown, comment, reference };
		try {
			hbase.putRows("videodynamiciy", rows, colfams, quals, values);
			hbase.putRows("videodynamicbakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (infoIsExist(inforowkey+"+iy", "video") == 0) { // 说明vodeoinfo不存在该记录，所以导入到videoinfo;
			   Importvideoinfo(inforowkey+"+iy");
			}
			try {
				Importvideodynamic(key1);
			} catch (Exception e) {
				e.printStackTrace();
			}
		rows = null;
		colfams = null;
		quals = null;
		values = null;
		source = null;

		url = null;
		name = null;
		updown = null;
		comment = null;
		System.gc();
	}

	public void ZongYiInfo(String source) {
	
		 category = "zongyi";
		String alias = "";
		String shoubo = "";
		String gengji = "";
	
		String danjitime = "";
		String ycomment = "";
		String url = "";
		free = "1";
	    sumplaycount = "-1";

		int indexurl = source.indexOf("info------------------");
		int endurl = source.indexOf("<!");
		url = source.substring(indexurl + 22, endurl);// 当前处理的电视剧的url.
		String mykey = url;

		// System.out.println(url);
		if (url.contains("/lib") == false) {
			int namest = source.indexOf("<title>");
			if (namest >= 0) {
				int nameend = source.indexOf("-", namest);
				name = source.substring(namest + 7, nameend).replaceAll(
						"&nbsp;", "");
				// System.out.println(name);
			}
			int indexht=source.indexOf("</html>");
			int commindex = source.indexOf("Comment", indexht);
			if (commindex >= 0) {
				int coudex = source.indexOf("\"count\":", commindex);
				int enddex = source.indexOf(",", coudex);
				if (enddex > coudex) {
					comment = source.substring(coudex + 8, enddex);
				}
				if (comment.length() > 12) {
					comment = "0";
				}
			}

			int ycommindex = source.indexOf("Ycomment", commindex);
			if (ycommindex >= 0) {
				int coudex = source.indexOf("\"count\":", ycommindex);
				int enddex = source.indexOf(",", coudex);
				if (enddex > coudex) {
					ycomment = source.substring(coudex + 8, enddex);
				}
				if (ycomment.length() > 12) {
					ycomment = "0";
				}
			}

			int upscost = source.indexOf("try{null",indexht);
			if (upscost >= 0) {
				int upst = source.indexOf("up\":", upscost);
				int upend = source.indexOf("}", upst);
				String up = source.substring(upst + 4, upend);
				int downst = source.indexOf("down\":", upscost);
				int downend = source.indexOf(",", downst);
				String down = source.substring(downst + 6, downend);
				updown = up + "@" + down;
				// System.out.println(updown);
				int scost = source.indexOf("score\":", upscost);
				int scoend = source.indexOf(",", scost);
				score = source.substring(scost + 7, scoend);
			}

			int actst = source.indexOf("<em>主持人：", namest);
			if (actst >= 0) {
				int actend = source.indexOf("</em>", actst);
				if (actend > actst) {
					String actor = source.substring(actst + 8, actend); // 主持人的所有信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = actor.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = actor.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String preactor = actor.substring(indexur,
									endur + 4);// 每个主持人的信息。
							int indexactor = preactor.indexOf("\">");
							int endactora = preactor
									.indexOf("</a>", indexactor);
							if (endactora >= indexactor) {
								String Act = preactor.substring(indexactor + 2,
										endactora); // 每个导演的名字。
								if (n == 1) {
									Acto.append(Act);
								} else {
									Acto.append("@" + Act);
								}
							}
							int indexactpro = preactor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = preactor.indexOf("\"",
										indexactpro + 10);
								if (endactpro > indexactpro + 9) {
									String Actpro = preactor.substring(
											indexactpro + 9, endactpro);// 每个主持人的URL.
									Acto.append("$" + Actpro);
								}
							}
						}
						n++;
					}
					actorlist = Acto.toString();
					// System.out.println(actorlist);
				}
			}

			actst = source.indexOf("<p class=\"li-mini\">主持人：", namest);
			if (actst >= 0) {
				int actend = source.indexOf("</p>", actst);
				if (actend > actst) {
					String actor = source.substring(actst + 8, actend); // 主持人的所有信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = actor.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = actor.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String preactor = actor.substring(indexur,
									endur + 4);// 每个主持人的信息。
							int indexactor = preactor.indexOf("\">");
							int endactora = preactor
									.indexOf("</a>", indexactor);
							if (endactora >= indexactor) {
								String Act = preactor.substring(indexactor + 2,
										endactora); // 每个导演的名字。
								if (n == 1) {
									Acto.append(Act);
								} else {
									Acto.append("@" + Act);
								}
							}
							int indexactpro = preactor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = preactor.indexOf("\"",
										indexactpro + 10);
								if (endactpro > indexactpro + 9) {
									String Actpro = preactor.substring(
											indexactpro + 9, endactpro);// 每个主持人的URL.
									Acto.append("$" + Actpro);
								}
							}
						}
						n++;
					}
					actorlist = Acto.toString();
					// System.out.println(actorlist);
				}
			}

			actst = source.indexOf("<p class=\"li-large\">主持人：", namest);
			if (actst >= 0) {
				int actend = source.indexOf("</p>", actst);
				if (actend > actst) {
					String actor = source.substring(actst + 8, actend); // 主持人的所有信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = actor.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = actor.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String preactor = actor.substring(indexur,
									endur + 4);// 每个主持人的信息。
							int indexactor = preactor.indexOf("\">");
							int endactora = preactor
									.indexOf("</a>", indexactor);
							if (endactora >= indexactor) {
								String Act = preactor.substring(indexactor + 2,
										endactora); // 每个导演的名字。
								if (n == 1) {
									Acto.append(Act);
								} else {
									Acto.append("@" + Act);
								}
							}
							int indexactpro = preactor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = preactor.indexOf("\"",
										indexactpro + 10);
								if (endactpro > indexactpro + 9) {
									String Actpro = preactor.substring(
											indexactpro + 9, endactpro);// 每个主持人的URL.
									Acto.append("$" + Actpro);
								}
							}
						}
						n++;
					}
					actorlist = Acto.toString();
					// System.out.println(actorlist);
				}
			}

			int locationst = source.indexOf("地区：", namest);
			if (locationst >= 0) {
				int locast = source.indexOf("\">", locationst);
				int locaend = source.indexOf("</a>", locast);
				if (locaend > locast) {
					area = source.substring(locast + 2, locaend);
				}
			}

			int indexarea = source.indexOf("<em>类型");
			if (indexarea >= 0) {
				int indexend = source.indexOf("</em>", indexarea);
				if (indexend > indexarea) {
					String tmparea = source.substring(indexarea, indexend);
					StringBuffer areau = new StringBuffer();
					int index = 0;
					int end = 0;
					int n = 1;
					while (index >= 0) {
						index = tmparea.indexOf("\">", end);
						end = tmparea.indexOf("</a>", index);
						if (index == -1) {
							break;
						}
						String tmpa = tmparea.substring(index + 2, end);
						if (n == 1) {
							areau.append(tmpa);
						} else {
							areau.append("@" + tmpa);
						}
						n++;
						if (n > 3)
							break;
					}
					type = areau.toString();
				}
			}

			int indexty = source.indexOf("<p class=\"li-large\">类型：",namest);
			if (indexty >= 0) {
				int endty = source.indexOf("</p>", indexty);
				if (endty > indexty) {
					String typee = source.substring(indexty, endty);
					int index = 0;
					int endindex = 0;
					int n = 1;
					StringBuffer typeu = new StringBuffer();
					while (index >= 0) {
						index = typee.indexOf("\">", endindex + 25);
						endindex = typee.indexOf("</a>", index);
						if (endindex > index && index >= 0) {
							String tmp = typee.substring(index + 2, endindex);
							if (n == 1) {
								typeu.append(tmp);
							} else {
								typeu.append("@" + tmp);
							}
						}
						n++;
						if (n > 3)
							break;
					}
					type = typeu.toString();
					// System.out.println(type);
				}
			}

			indexty = source.indexOf("<p class=\"li-mini\">类型：");
			if (indexty >= 0) {
				int endty = source.indexOf("</p>", indexty);
				if (endty > indexty) {
					String typee = source.substring(indexty, endty);
					int index = 0;
					int endindex = 0;
					int n = 1;
					StringBuffer typeu = new StringBuffer();
					while (index >= 0) {
						index = typee.indexOf("\">", endindex + 25);
						endindex = typee.indexOf("</a>", index);
						if (endindex > index && index >= 0) {
							String tmp = typee.substring(index + 2, endindex);
							if (n == 1) {
								typeu.append(tmp);
							} else {
								typeu.append("@" + tmp);
							}
						}
						n++;
						if (n > 3)
							break;
					}
					type = typeu.toString();
					// System.out.println(type);
				}
			}

			int tvst = source.indexOf("<em>电视台：",namest);
			if (tvst >= 0) {
				int tvend = source.indexOf("</em>", tvst);
				if (tvend > tvst) {
					tv = source.substring(tvst + 7, tvend).replaceAll(
							"\\s*：\\s*", "");
					// System.out.println(tv);
				}
			}
			int genst = source.indexOf("<em>更新至：", tvst);
			if (genst >= 0) {
				int tvend = source.indexOf("</em>", genst);
				if (tvend >= genst) {
					String gengcon = source.substring(genst, tvend);
					int gnst = gengcon.indexOf("\">");
					int gnend = gengcon.indexOf("</a>");
					gengji = gengcon.substring(gnst + 2, gnend);
					// System.out.println(gengji);
				}
			}

			int lanst = source.indexOf("<em>语言：", tvst);
			if (lanst >= 0) {
				int lanend = source.indexOf("</em>", lanst);
				if (lanend >= lanst) {
					String lancon = source.substring(lanst, lanend);
					int indextype = 0;
					int endtype = 0;
					StringBuffer lanu = new StringBuffer();
					int n = 1;
					while (indextype >= 0) {
						indextype = lancon.indexOf("\">", endtype); // endur已经是最后一个是需要检测indexur.
						endtype = lancon.indexOf("</a>", indextype);
						if (indextype >= 0 && endtype > indextype) {
							String typee = lancon.substring(indextype + 2,
									endtype);
							if (n == 1) {
								lanu.append(typee);
							} else {
								lanu.append("@" + typee);
							}
							n++;

						}
					}
					lan = lanu.toString();
					// System.out.println(lan);
				}
			}

			int indexdescrip = source
					.indexOf(" <meta itemprop=\"description\" content=\"",namest);
			if (indexdescrip > -1) {
				String indescri = source.substring(indexdescrip);
				int indexdes = indescri.indexOf("content=\"");
				int enddes = indescri.indexOf("/>");
				if (enddes > indexdes && indexdes >= 0) {
					String indestmp = indescri.substring(indexdes + 9, enddes);
					summarize = indestmp.replaceAll("\"", "");
					// System.out.println(summarize);// 描述。
				}
			}

			int indexdescr = source.lastIndexOf("简介：</em>",namest);
			if (indexdescr > -1) {
				int endd = source.indexOf("</span>", indexdescr);
				if (endd > indexdescr) {
					summarize = source.substring(indexdescr + 8, endd);
					// System.out.println(summarize);
				}
			}

		} else { // url包含lib
			int namest = source.indexOf("节目名");
			if (namest >= 0) {
				int nameend = source.indexOf(";", namest);
				if (nameend >= namest) {
					name = source.substring(namest + 4, nameend);
				}
			}
			int alist = source.indexOf("dib\">", namest);
			if (alist >= 0) {
				int aliend = source.indexOf("</em>", alist);
				if (aliend >= alist) {
					alias = source.substring(alist + 5, aliend);
				}
			}
			int shoust = source.indexOf("首播时间", namest);
			if (shoust >= 0) {
				int shouend = source.indexOf("</em>", shoust);
				if (shouend >= shoust) {
					shoubo = source.substring(shoust + 5, shouend);
				}
			}
			int lanst = source.indexOf("语言", shoust);
			if (lanst >= 0) {
				int lanend = source.indexOf("</em>", lanst);
				if (lanend >= lanst) {
					lan = source.substring(lanst + 3, lanend);
				}
			}
			int locst = source.indexOf("地区", shoust);
			if (locst >= 0) {
				int locast = source.indexOf(">", locst);
				if (locast >= 0) {
					int locaend = source.indexOf("</a>", locast);
					if (locaend >= locast) {
						area = source.substring(locast + 1, locaend);
					}
				}
			}
			int tyst = source.indexOf("类型", locst);
			if (tyst >= 0) {
				StringBuffer typebuf = new StringBuffer();
				int tyend = source.indexOf("</em>", tyst);
				if (tyend >= tyst) {
					String typecon = source.substring(tyst, tyend);
					int typest = 0;
					int typeend = 0;
					int n = 1;
					while (typest >= 0) {
						String typepre = null;
						int typprst = typecon.indexOf("title=\"\" >", typeend);
						if (typprst >= 0) {
							int typprend = typecon.indexOf("</a>", typprst);
							if (typprend >= typprst) {
								typepre = typecon.substring(typprst + 10,
										typprend);
							}
						}
						if (n == 1) {
							typebuf.append(typepre);
						} else {
							typebuf.append("@" + typepre);
						}
					}
					type = typebuf.toString();
				}
			}
			int gengjist = source.indexOf("更新集数", namest);
			if (gengjist >= 0) {
				int gengjiend = source.indexOf("</em>", gengjist);
				if (gengjiend >= gengjist) {
					gengji = source.substring(gengjist + 5, gengjiend);
				}
			}
			int gengtimest = source.indexOf("更新时间", gengjist);
			if (gengtimest >= 0) {
				int gengtimeend = source.indexOf("</em>", gengtimest);
				if (gengtimeend >= gengtimest) {
					year = source.substring(gengtimest + 5, gengtimeend);
				}
			}

			int danjitimest = source.indexOf("单集时长", gengtimest);
			if (danjitimest >= 0) {
				int gengtimeend = source.indexOf("</em>", danjitimest);
				if (gengtimeend >= danjitimest) {
					danjitime = source.substring(danjitimest + 5, gengtimeend);
				}
			}

			int tvst = source.indexOf("电视台", namest);
			if (tvst >= 0) {
				int gengtimeend = source.indexOf("</em>", tvst);
				if (gengtimeend >= tvst) {
					tv = source.substring(tvst + 4, gengtimeend);
				}
			}
			int desst = source.indexOf("片名简介", danjitimest);
			if (desst >= 0) {
				int desend = source.indexOf("/>", desst);
				if (desend >= desst) {
					summarize = source.substring(desst + 5, desend);
				}
			}

			int upscost = source.indexOf("try{", desst);
			if (upscost >= 0) {
				int upst = source.indexOf("up\":", upscost);
				int upend = source.indexOf(",", upst);
				if (upend > upst && upst >= 0) {
					String up = source.substring(upst + 4, upend);
					int downst = source.indexOf("down\":", upend);
					int downend = source.indexOf(",", downst);
					String down = source.substring(downst + 6, downend);
					updown = up + "@" + down;
					int scost = source.indexOf("score\":", downend);
					int scoend = source.indexOf(",", scost);
					score = source.substring(scost + 7, scoend);
				}
			}
		}
		String key1 = mykey + "+" + "iy";
		String key2 = mykey + "+" + "iy+" + timee;
		String[] rows = null;
		String[] colfams = null;
		String[] quals = null;
		String[] values = null;
		rows = new String[] { key1, key1, key1, key1, key1, key1, key1, key1,
				key1, key1, key1, key1, key1, key1, key1, key1 };
		colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B", "B",
				"B", "B", "B", "B", "B", "B", "C" };
		quals = new String[] { "inforowkey", "year", "website", "category",
				"name", "Actor", "location", "tv", "summarize", "alias",
				"shoubo", "language", "type", "gengji", "danjitime",
				"crawltime" };
		values = new String[] { mykey, year, "iy", category, name, actorlist,
				area, tv, summarize, alias, shoubo, lan, type, gengji,
				danjitime, IqiyiParse.crawltime };
		try {
			hbase.putRows("movieinfoiy", rows, colfams, quals, values);
			hbase.putRows("movieinfobakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		rows = new String[] { key2, key2, key2, key2, key2, key2, key2, key2,
				key2, key2, key2 };
		colfams = new String[] { "R", "R", "R", "B", "C", "C", "C", "C", "C",
				"C", "C" };
		quals = new String[] { "inforowkey", "year", "website", "category",
				"name", "score", "updown", "comment", "ycomment", "free",
				"sumplaycount" };
		values = new String[] { mykey, year, "iy", category, name, score,
				updown, comment, ycomment, free, sumplaycount };
		try {
			hbase.putRows("moviedynamiciy", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbakiy", rows, colfams, quals, values);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (infoIsExist(key1, "movie") == 0) { // 说明movieinfo不存在该记录，所以导入到movieinfo;
			Importmovieinfo(key1);
		}
		try {
			Importmoviedynamic(key1, category);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
		rows = null;
		colfams = null;
		quals = null;
		values = null;
		source = null;
		category = null;
		name = null;
		
		free = null;
		sumplaycount = null;
		area = null;
		tv = null;
		summarize = null;
		alias = null;
		shoubo = null;
	
		type = null;
		gengji = null;
	
		danjitime = null;
		updown = null;
		comment = null;
		ycomment = null;

		score = null;
		System.gc();
	}

	public void ZongYiItem(String source,JDBCConnection jdbc) {

		String mykey = IqiyiParse.parentId;
	    showtype = "正片";
		String alias = "";
		String jpgurl = "";
		String ppre = "";
		String mpre = "";
		String ycomment = "";
		int indexurl = source.indexOf("info------------------");
		int endurl = source.indexOf("<!");
		String url = source.substring(indexurl + 22, endurl);// 当前处理电影的url.
		int indexname = source.indexOf("<title>");
		if (indexname >= 0) {
			int nameend = source.indexOf("-", indexname);
			name = source.substring(indexname + 7, nameend).replaceAll(
					"&nbsp;", "");
			// System.out.println(name);
		}
		
		int indexht=source.indexOf("</html>");
		int commindex = source.indexOf("Comment", indexht);
		if (commindex >= 0) {
			int coudex = source.indexOf("\"count\":", commindex);
			int enddex = source.indexOf(",", coudex);
			if (enddex > coudex + 8) {
				comment = source.substring(coudex + 8, enddex);
			}
			if (comment.length() > 12) {
				comment = "0";
			}
		}

		int ycommindex = source.indexOf("Ycomment", commindex);
		if (ycommindex >= 0) {
			int coudex = source.indexOf("\"count\":", ycommindex);
			int enddex = source.indexOf(",", coudex);
			if (enddex > coudex + 8) {
				ycomment = source.substring(coudex + 8, enddex);
			}
			if (ycomment.length() > 12) {
				ycomment = "0";
			}
		}

		int indexty = source.indexOf("id=\"widget-videotag\">", indexname);
		if (indexty > 0) {
			String contype = source.substring(indexty);
			int indetype = contype.indexOf("id=\"widget-videotag\">");
			int endyear = contype.indexOf("</span>");
			String sourceype = "";
			if (indetype < endyear && indetype >= 0) {
				sourceype = contype.substring(indetype + 22, endyear);
			}
			int indextyp = 0;
			int endtyp = 0;
			StringBuffer Type = new StringBuffer();
			int n = 1;
			while (indextyp >= 0) {
				indextyp = sourceype.indexOf("class=\"green\">", endtyp); // endur已经是最后一个是需要检测indexur.
				endtyp = sourceype.indexOf("</a>", indextyp);
				if (indextyp >= 0) {
					String type = sourceype.substring(indextyp + 14, endtyp);
					if (n == 1) {
						Type.append(type);
					} else {
						Type.append("@" + type);
					}
					n++;

				}
			}
			type = Type.toString();
		}

		int indexali = source.indexOf("<meta itemprop=\"alias\"", indexname);
		if (indexali > 0) {
			String contali = source.substring(indexali);
			int indexalias = contali.indexOf("content=\"");
			int endalias = contali.indexOf("/>");
			if (endalias > indexalias) {
				String alia = contali.substring(indexalias + 9, endalias);
				alias = alia.replaceAll("\"", "");
			}
		}

		int indexdiract = source
				.indexOf("<p id=\"widget-director\" rseat=\"导演\">",indexname);
		if (indexdiract > 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("rseat=\"导演\">");
			int endAre1 = contmp1.indexOf("</p>");
			if (indexar1 < endAre1 && indexar1 >= 0) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						String Act = actor.substring(indexactor + 2, endactora); // 每个导演的名字。
						if (n == 1) {
							Acto.append(Act);
						} else {
							Acto.append("@" + Act);
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							String Actpro = actor.substring(indexactpro + 9,
									endactpro);// 每个导演的URL.
							Acto.append("$" + Actpro);
						}
					}
					n++;
				}
				director = Acto.toString();
			}
		}
		indexdiract = source.indexOf("<p id=\"datainfo-director\"", indexname);
		if (indexdiract >= 0) {
			String contmp1 = source.substring(indexdiract);
			int indexar1 = contmp1.indexOf("<p id=\"datainfo-director\"");
			int endAre1 = contmp1.indexOf("</p>");
			if (endAre1 > -1 && endAre1 > indexar1) {
				String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Acto = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
					endur = sourcepar.indexOf("</a>", indexur);
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个导演的信息。
						int indexactor = actor.indexOf("\">");
						int endactora = actor.indexOf("</a>", indexactor);
						if (endactora > indexactor && indexactor >= 0) {
							String Act = actor.substring(indexactor + 2,
									endactora); // 每个导演的名字。
							if (n == 1) {
								Acto.append(Act);
							} else {
								Acto.append("@" + Act);
							}
						}
						int indexactpro = actor.indexOf("<a href=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\"",
									indexactpro + 10);
							if (endactpro > indexactpro) {
								String Actpro = actor.substring(
										indexactpro + 9, endactpro);// 每个导演的URL.
								Acto.append("$" + Actpro);
							}
						}
					}
					n++;
				}
				director = Acto.toString();
				// // System.out.println("导演："+Director);
			}
		}

		int indexdescrip = source.indexOf("<meta itemprop=\"description\"",indexname);
		if (indexdescrip > -1) {
			String indescri = source.substring(indexdescrip);
			int indexdes = indescri.indexOf("content=\"");
			if (indexdes >= 0) {
				int enddes = indescri.indexOf("/>", indexdes);
				if (enddes > indexdes + 9) {
					String indestmp = indescri.substring(indexdes + 9, enddes);
					summarize = indestmp.replaceAll("\"", "");
					// // System.out.println(description);//描述。
				}
			}

			String ttime = null;// 后面取顶踩数时需要。
			int indexdur = source.indexOf("<meta itemprop=\"duration\"",
					indexname);
			if (indexdur > -1) {
				String indedu = source.substring(indexdur);
				int indexdura = indedu.indexOf("content=\"");
				int enddes = indedu.indexOf("/>");
				if (enddes > indexdura && indexdura >= 0) {
					String indestmp = indedu.substring(indexdura + 9, enddes);
					ttime = indestmp.replaceAll("\\D*", "");
					int second = Integer.parseInt(ttime);
					int minute = second / 60;
					duration = Integer.toString(minute);
				}
			}

			int indexloc = source.indexOf("<meta itemprop=\"sourceLocation\"",
					indexname);
			if (indexloc > -1) {
				String indesloc = source.substring(indexloc);
				int indeloc = indesloc.indexOf("content=\"");
				int enddes = indesloc.indexOf("/>");
				String indestmp = indesloc.substring(indeloc + 9, enddes);
				area = indestmp.replaceAll("\"", "");
			}

			int indexlan = source.indexOf("<meta itemprop=\"inLanguage\"",
					indexloc);
			if (indexlan > -1) {
				String indeslan = source.substring(indexlan);
				int indexla = indeslan.indexOf("content=\"");
				int enddes = indeslan.indexOf("/>");
				String indestmp = indeslan.substring(indexla + 9, enddes);
				lan = indestmp.replaceAll("\"", "");
			}

			int indexact = source
					.indexOf("<div class=\"peos-info\" id=\"widget-actor\">",indexname);
			if (indexact > 0) {
				String contmp1 = source.substring(indexact);
				int indexar1 = contmp1.indexOf("id=\"widget-actor\">");
				int endAre1 = contmp1.indexOf("</div>");
				String sourcepar = contmp1.substring(indexar1 + 18, endAre1); // sourcepar是导演信息。
				int indexur = 0;
				int endur = 0;
				int n = 1;
				StringBuffer Actjue = new StringBuffer();
				while (indexur >= 0) {
					indexur = sourcepar.indexOf("<p>", endur);
					endur = sourcepar.indexOf("</p>", indexur);// endur已经是最后一个是需要检测indexur.
					if (indexur >= 0) {
						String actor = sourcepar.substring(indexur, endur + 4);// 每个演员的信息。
						int indexactpro = actor.indexOf("rseat=\"");
						if (indexactpro >= 0) {
							int endactpro = actor.indexOf("\">", indexactpro);
							String Actpro = actor.substring(indexactpro + 7,
									endactpro);// 每个演员的名字.
							if (n == 1) {
								Actjue.append(Actpro);
							} else {
								Actjue.append("@" + Actpro);
							}
						}
						int indexjue = actor
								.indexOf(">饰</span><span class=\"ml5 fs12 c-666\" rseat=\"");
						if (indexjue >= 0) {
							int endjue = actor.indexOf("\">", indexjue);
							String Actju = actor.substring(indexjue + 45,
									endjue);
							String jue = Actju.replaceAll("角色_", "饰");
							Actjue.append("$" + jue);
						}
						int indexactor = actor.indexOf("<a href=\"");
						if (indexactor > -1) {
							int endactora = actor.indexOf("\"", indexactor + 9);
							String Act = actor.substring(indexactor + 9,
									endactora); // 每个演员的连接.
							Actjue.append("$" + Act);
						}
						n++;
					}
				}
				actorlist = Actjue.toString();
			}

			indexdiract = source.indexOf("<p id=\"datainfo-actor\"", indexdiract);
			if (indexdiract >= 0) {
				String contmp1 = source.substring(indexdiract);
				int indexar1 = contmp1.indexOf("<p id=\"datainfo-actor\"");
				int endAre1 = contmp1.indexOf("</p>");
				if (endAre1 > -1) {
					String sourcepar = contmp1.substring(indexar1, endAre1); // sourcepar是导演信息。
					int indexur = 0;
					int endur = 0;
					int n = 1;
					StringBuffer Acto = new StringBuffer();
					while (indexur >= 0) {
						indexur = sourcepar.indexOf("<a href=\"", endur); // endur已经是最后一个是需要检测indexur.
						endur = sourcepar.indexOf("</a>", indexur);
						if (indexur >= 0) {
							String actor = sourcepar.substring(indexur,
									endur + 4);// 每个导演的信息。
							int indexactor = actor.indexOf("\">");
							int endactora = actor.indexOf("</a>", indexactor);
							if (endactora > indexactor && indexactor >= 0) {
								String Act = actor.substring(indexactor + 2,
										endactora); // 每个导演的名字。
								if (n == 1) {
									Acto.append(Act);
								} else {
									Acto.append("@" + Act);
								}
							}
							int indexactpro = actor.indexOf("<a href=\"");
							if (indexactpro >= 0) {
								int endactpro = actor.indexOf("\"",
										indexactpro + 10);
								if (endactpro > indexactpro) {
									String Actpro = actor.substring(
											indexactpro + 9, endactpro);// 每个导演的URL.
									Acto.append("$" + Actpro);
								}
							}
						}
						n++;
					}
					actorlist = Acto.toString();
					// System.out.println("主演：" + actorlist);
				}
			}

			int indexpre = source.indexOf("{playCountPCMobileCb", ycommindex);
			if (indexpre > -1) {
				int indexp = source.indexOf("p\":", indexpre);
				int endp = source.indexOf(",", indexp);
				if (endp > indexp) {
					ppre = source.substring(indexp + 3, endp);
					// System.out.println("PC占比：" + ppre + "%");
				}
				int indexm = source.indexOf("m\":", endp);
				int endm = source.indexOf("}", indexm);
				if (endm > indexm) {
					mpre = source.substring(indexm + 3, endm);
					// System.out.println("移动占比：" + mpre + "%");
				}
			}

			int indexx = source
					.indexOf("try{window.Q.__callbacks__.", indexpre);
			int endd = source.indexOf("})", indexx);
			if (endd > indexx && indexx >= 0) {
				String content = source.substring(indexx, endd);
				int index = 0;
				int end = 0;
				int n = 1;
				String urll = "";
				StringBuffer urla = new StringBuffer();
				while (index >= 0) {
					index = content.indexOf("\"albumUrl\":\"", end);
					if (index >= 0) {
						end = content.indexOf("\",", index);
						if (end > index) {
							urll = content.substring(index + 12, end);
						}

						if (n == 1) {
							urla.append(urll);
						} else {
							urla.append("@@" + urll);
						}
					}

					n++;
				}
				reference = urla.toString();
				// System.out.println(reference);
			}

			int indexsumm = source.indexOf(
					"try{window.Q.__callbacks__.cbgt6rz7(", indexx);
			if (indexsumm >= 0) {
				String sumplaycounttmp = "";
				int indexsum = source.indexOf(":", indexsumm);
				int end = source.indexOf("}", indexsum);
				if (end > indexsum) {
					sumplaycounttmp = source.substring(indexsum + 1, end);
				}

				if (sumplaycounttmp.contains("万")) {
					double sumplaycountint = Double.parseDouble(sumplaycounttmp
							.replaceAll("万", "")) * 10000;
					BigDecimal big = new BigDecimal(sumplaycountint);
					sumplaycount = big.toString().replaceAll("\\.0", "");
				} else if (sumplaycounttmp.contains("亿")) {
					Long sumplaycountint = Math
							.round(Double.parseDouble(sumplaycounttmp
									.replaceAll("亿", "")) * 100000000);
					BigDecimal big = new BigDecimal(sumplaycountint);
					sumplaycount = big.toString().replaceAll("\\.0", "");
				} else {
					sumplaycount = sumplaycounttmp;
				}

				// System.out.println("总播放数" + sumplaycount);
			}
			
			if(sumplaycount==null) {
				jdbc.log("李辉", url+"+iy", 1, "iy", url, "播放量没有抓到", 2);
				return;
				}

			int upscost = source.indexOf("try{null", indexsumm);
			if (upscost >= 0) {
				int upst = source.indexOf("up\":", upscost);
				int upend = source.indexOf("}", upst);
				String up = source.substring(upst + 4, upend);
				int downst = source.indexOf("down\":", upscost);
				int downend = source.indexOf(",", downst);
				String down = source.substring(downst + 6, downend);
				updown = up + "@" + down;
				// System.out.println(updown);
				int scost = source.indexOf("score\":", upscost);
				int scoend = source.indexOf(",", scost);
				score = source.substring(scost + 7, scoend);
			}
			inforowkey=mykey;
            playrowkey=url;
			String key1 = mykey + "+" + url + "+iy";
			String key2 = mykey + "+" + url + "+iy+" + timee;
			String[] rows = null;
			String[] colfams = null;
			String[] quals = null;
			String[] values = null;
			rows = new String[] { key1, key1, key1, key1, key1, key1, key1,
					key1, key1, key1, key1, key1, key1, key1, key1, };
			colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B",
					"B", "B", "B", "B", "B", "B", "C" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"showtype", "name", "type", "alias", "director",
					"description", "jpgurl", "minute", "location", "language",
					"Actor", "crawltime" };
			values = new String[] { mykey, url, "iy", showtype, name, type,
					alias, director, summarize, jpgurl, duration, area,
					lan, actorlist, IqiyiParse.crawltime };
			try {
				hbase.putRows("videoinfoiy", rows, colfams, quals, values);
				hbase.putRows("videoinfobakiy", rows, colfams, quals, values);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			rows = new String[] { key2, key2, key2, key2, key2, key2, key2,
					key2, key2, key2, key2, key2 };
			colfams = new String[] { "R", "R", "R", "C", "C", "C", "C", "C",
					"C", "C", "C", "C" };
			quals = new String[] { "inforowkey", "playrowkey", "website",
					"name", "sumplaycount", "updown", "mobliePrecentge",
					"pcPrecentge", "coment", "ycomment", "reference", "score" };
			values = new String[] { mykey, url, "iy", name, sumplaycount,
					updown, mpre, ppre, comment, ycomment, reference, score };
			try {
				hbase.putRows("videodynamiciy", rows, colfams, quals, values);
				hbase.putRows("videodynamicbakiy", rows, colfams, quals, values);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if (infoIsExist(inforowkey+"+iy", "video") == 0) { // 说明vodeoinfo不存在该记录，所以导入到videoinfo;
				   Importvideoinfo(inforowkey+"+iy");
				}
				try {
					Importvideodynamic(key1);
				} catch (Exception e) {
					e.printStackTrace();
				}
			rows = null;
			colfams = null;
			quals = null;
			values = null;
			source = null;
			showtype = null;
			name = null;
			type = null;
			// time = null;
			area = null;
			director = null;
			summarize = null;
			comment = null;
			ycomment = null;
			alias = null;
			lan = null;
			updown = null;
			ppre = null;
			mpre = null;
			jpgurl = null;
			score = null;
			actorlist = null;
		
			System.gc();
		}
	}
	

	public int infoIsExist(String rowkey, String tabletype) {
		int count = 0;
		ResultSet rs = jdbconn.executeQuerySingle("select count(*) from "
				+ tabletype + "info where rowkey = '" + rowkey + "'");
		try {
			while (rs.next()) {
				String cou = rs.getString(1);
				count = Integer.parseInt(cou);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return count;
	}
	
	
	public void Importmovieinfo(String key) {
		ArrayList<TextValue> value = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = key;
		value.add(rowkeytv);

		TextValue yeartv = new TextValue();
		yeartv.text = "year";
		yeartv.value = year;
		value.add(yeartv);

		TextValue timetv = new TextValue();
		timetv.text = "time";
		timetv.value = time;
		value.add(timetv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		nametv.value = name;
		value.add(nametv);

		TextValue summtv = new TextValue();
		summtv.text = "summarize";
		summtv.value = summarize;
		value.add(summtv);

		TextValue lantv = new TextValue();
		lantv.text = "lan";
		lantv.value = lan;
		value.add(lantv);

		TextValue areatv = new TextValue();
		areatv.text = "area";
		areatv.value = area;
		value.add(areatv);

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		duratv.value = duration;
		value.add(duratv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = -1;
		value.add(pricetv);

		String[] directorSplits = director.split("@");
		if (directorSplits != null) {
			for (int i = 0; i < directorSplits.length; i++) {
				TextValue directortv = new TextValue();
				directortv.text = "director" + (i + 1);
				if (directorSplits[i].indexOf("$") > 0) {
					directorSplits[i] = directorSplits[i].substring(0,
							directorSplits[i].indexOf("$"));
				}
				if (directorSplits[0] == "") {
					return;
				}
				directortv.value = directorSplits[i];
				value.add(directortv);
				if (i == 2)
					break;
			}
		}

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		categorytv.value = category;
		value.add(categorytv);

		TextValue crawlt = new TextValue();
		crawlt.text = "crawltime";
		crawlt.value = IqiyiParse.crawltime;
		value.add(crawlt);
		
		TextValue playtv = new TextValue();
		playtv.text = "playtv";
		playtv.value = tv;
		value.add(playtv);
		
		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "iy";
		value.add(namewebsite);

		String[] typeSplits = type.split("@");
		for (int i = 0; i < typeSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i + 1);
			typetv.value = typeSplits[i];
			if (typeSplits[0] == "") {
				break;
			}
			value.add(typetv);
			if (i == 2)
				break;
		}

		String[] mainactorSplits = actorlist.split("@");
		if (mainactorSplits != null) {

			for (int i = 0; i < mainactorSplits.length; i++) {
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				if (mainactorSplits[i].indexOf("$") > 0) {
					mainactorSplits[i] = mainactorSplits[i].substring(0,
							mainactorSplits[i].indexOf("$"));
				}
				actortv.value = mainactorSplits[i];
				if (mainactorSplits[0] == "") {
					break;
				}
				value.add(actortv);
				if (i == 2)
					break;
			}
		}
		// System.out.println(name + "导入成功");
		jdbconn.insert(value, "movieinfo");
	
	}
	
	
	public void Importvideoinfo(String key) {
		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = key;
		values.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "name";
		nametv.value = name;
		values.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "iy";
		values.add(namewebsite);

		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = inforowkey;
		values.add(inforowkeytv);

		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = playrowkey;
		values.add(playrowkeytv);

		TextValue crawlt = new TextValue();
		crawlt.text = "crawltime";
		crawlt.value = IqiyiParse.crawltime;
		values.add(crawlt);

		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = showtype;
		values.add(showtypetv);
		jdbconn.insert(values, "videoinfo");
	
		
	}
	
	


	public void Importmoviedynamic(String keyString, String categoryString)   
			throws Exception { // keyString is key of movieinfo
		if (!categoryString.equals("zongyi") && !categoryString.equals("movie")
				&& !categoryString.equals("tv")
				&& !categoryString.equals("dongman"))
			return;
						if (!reference.equals("")) {
							// parse
							String refer[] = reference.split("@@");
							for (int i = 0; i < refer.length; i++) {
								 moviereference(refer[i] + "#vfrm=2-4-0-1", sd);
							}
						}
         		int up = -1;
				int down = -1;
				int updownindex = updown.indexOf("@");
				if (updownindex >= 0) {
					String upcount = updown.substring(0, updownindex);
					String downcount = updown.substring(updownindex + 1,
							updown.length());
					up = ConvertToInt(upcount);
					down = ConvertToInt(downcount);
				}
			
			   int sumplay=-1;
			    if(!sumplaycount.equals("")) {
				 sumplay = ConvertToInt(sumplaycount);
				if (sumplay < 0)
					sumplay = -1;
			    }

				
				double scorere=-1;
					if(!score.equals("")) {
					scorere = Double.parseDouble(score);
					}
			
					int	commentCount=-1;
                  if(!comment.equals("")) {			
				   commentCount = ConvertToInt(comment);
                  }
			

				int freeCount = 1;
				if(!free.equals("")) {
                freeCount = ConvertToInt(free);
				}

			try {

				ArrayList<TextValue> values = new ArrayList<TextValue>();
				TextValue rowkeytv = new TextValue();
				rowkeytv.text = "rowkey";
				rowkeytv.value = keyString;
				values.add(rowkeytv);
				TextValue sumPlayCounttv = new TextValue();
				sumPlayCounttv.text = "sumPlayCount";
				sumPlayCounttv.value = sumplay;
				values.add(sumPlayCounttv);
				TextValue scoretv = new TextValue();
				scoretv.text = "score";
				scoretv.value = scorere;
				values.add(scoretv);

				TextValue todaytv = new TextValue();
				todaytv.text = "todayPlayCount";
				todaytv.value = -1;
				values.add(todaytv);

				TextValue mantv = new TextValue();
				mantv.text = "man";
				mantv.value = -1;
				values.add(mantv);

				TextValue womentv = new TextValue();
				womentv.text = "women";
				womentv.value = -1;
				values.add(womentv);

				TextValue uptv = new TextValue();
				uptv.text = "up";
				uptv.value = up;
				values.add(uptv);

				TextValue downtv = new TextValue();
				downtv.text = "down";
				downtv.value = down;
				values.add(downtv);

				TextValue categorytv = new TextValue();
				categorytv.text = "category";
				categorytv.value = categoryString;
				values.add(categorytv);

				TextValue commenttv = new TextValue();
				commenttv.text = "comment";
				commenttv.value = commentCount;// n 2:y
				values.add(commenttv);

				TextValue timestamptv = new TextValue();
				timestamptv.text = "timestamp";
				timestamptv.value = timestamp.substring(0, 10);
				values.add(timestamptv);

				TextValue namewebsite = new TextValue();
				namewebsite.text = "website";
				namewebsite.value = "iy";
				values.add(namewebsite);

				TextValue flag = new TextValue();
				flag.text = "flag";
				flag.value = "1";
				values.add(flag);

				TextValue freetv = new TextValue();
				freetv.text = "free";
				freetv.value = freeCount;// n 2:y
				values.add(freetv);

				if (!categoryString.equals("zongyi")) {
					jdbconn.insert(values, "moviedynamic" + sd);
				} else {
					jdbconn.insert(values, "cacheiy");
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			}
		
	
	

	public void Importvideodynamic(String keyString) throws Exception { // keyString																	// is
		// key of
		// videoinfo;
		// 父URL+子URL+“iy”(预告片除外)

					if (!reference.equals("")) {
						// parse
					
						String refer[] = reference.split("@@");
						for (int i = 0; i < refer.length; i++) {
							if (refer[i].contains("/a_")) {
								videoreference(refer[i] + "#vfrm=2-4-0-1+iy", sd); // 此时表示分集推荐的为信息页的URL，所以构造rowkey。
							} else if (!refer[i].contains("/v_")
									&& refer[i].length() < 45) {
								videoreference(refer[i] + "#vfrm=2-4-0-1+iy", sd);
							} else {
								videoreference(refer[i], sd); // 此时为playrowkey.
							}
						}
					}
			
				String inforowkeyString = inforowkey;
          
				String playkeyString = playrowkey;

				int sumplay = -1;
			if(!sumplaycount.equals("")) {
					sumplay = ConvertToInt(sumplaycount);
			}
		
				int up = -1;
				int down = -1;
			
					int updownindex = updown.indexOf("@");
					if (updownindex >= 0) {
						String upcount = updown.substring(0, updownindex);
						String downcount = updown.substring(
								updownindex + 1, updown.length());
						up = ConvertToInt(upcount);
						down = ConvertToInt(downcount);
					}
			

			int commentCount=-1;
			       if(!comment.equals("")) {
					commentCount = ConvertToInt(comment);
			       }
			
              try {
				ArrayList<TextValue> values = new ArrayList<TextValue>();
				
				TextValue rowkeytv = new TextValue();
				rowkeytv.text = "rowkey";
				rowkeytv.value = inforowkey+"+iy";
				values.add(rowkeytv);

				TextValue infokey = new TextValue();
				infokey.text = "inforowkey";
				infokey.value = inforowkeyString;
				values.add(infokey);

				TextValue playkey = new TextValue();
				playkey.text = "playrowkey";
				playkey.value = playkeyString;
				values.add(playkey);

				TextValue sumPlayCounttv = new TextValue();
				sumPlayCounttv.text = "sumPlayCount";
				sumPlayCounttv.value = sumplay;
				values.add(sumPlayCounttv);

				TextValue uptv = new TextValue();
				uptv.text = "up";
				uptv.value = up;
				values.add(uptv);

				TextValue downtv = new TextValue();
				downtv.text = "down";
				downtv.value = down;
				values.add(downtv);

				TextValue collecttv = new TextValue();
				collecttv.text = "collect";
				collecttv.value = -1;
				values.add(collecttv);

				TextValue outsidetv = new TextValue();
				outsidetv.text = "outside";
				outsidetv.value = -1;
				values.add(outsidetv);

				TextValue commenttv = new TextValue();
				commenttv.text = "comment";
				commenttv.value = commentCount;
				values.add(commenttv);
             
				TextValue timestamptv = new TextValue();
				timestamptv.text = "timestamp";
				timestamptv.value = timestamp.substring(0,10);
				values.add(timestamptv);

				TextValue namewebsite = new TextValue();
				namewebsite.text = "website";
				namewebsite.value = "iy";
				values.add(namewebsite);

				TextValue flag = new TextValue();
				flag.text = "flag";
				flag.value = "1";
				values.add(flag);


				if (jdbconn.insert(values, "videodynamic" + sd) == -1) {
					// System.out.println("insert error");
				}
              } catch(Exception e) {
            	  e.printStackTrace();
              }
			}
	
	
	

	public static void moviereference(String infourl, String sd) {

		String table = "moviedynamic" + sd;
		String key = infourl + "@@" + table;
		synchronized (movielist) {
			int done = 0;
			for (int i = 0; i < movielist.size(); i++) {
				TextValue tv = (TextValue) movielist.get(i);
				if (tv.text.equals(key)) {
					tv.value = Integer.parseInt(tv.value.toString()) + 1;
					done = 1;
					break;
				}
			}
			if (done == 0) {
				TextValue tv = new TextValue();
				tv.text = key;
				tv.value = 1;
				movielist.add(tv);
			}
		}
	}
	
	
	public static void videoreference(String infourl, String sd) {

		String table = "videodynamic" + sd;
		String key = infourl + "@@" + table;
		synchronized (movielist) {
			int done = 0;
			for (int i = 0; i < movielist.size(); i++) {
				TextValue tv = (TextValue) movielist.get(i);
				if (tv.text.equals(key)) {
					tv.value = Integer.parseInt(tv.value.toString()) + 1;
					done = 1;
					break;
				}
			}
			if (done == 0) {
				TextValue tv = new TextValue();
				tv.text = key;
				tv.value = 1;
				movielist.add(tv);
			}
		}
	}
	
	
	public  void referencemovie() {   //存moviedynamic;
		JDBCConnection conn = new JDBCConnection();
		for (int i = 0; i < movielist.size(); i++) {
			TextValue tv = (TextValue) movielist.get(i);
			String[] splits = tv.text.split("@@");
			if (splits != null && splits.length == 2) {
				String rowkey = splits[0] + "+iy";
				String tableName = splits[1];
				if (tableName.indexOf("videodynamic") >= 0) {
					continue;
				}
				String date = tableName.substring(tableName.indexOf("2"));
				String count = tv.value.toString();
				String website = "iy";
				String sql = "insert into reference" + date
						+ " (rowkey,reference,website) values ('" + rowkey
						+ "','" + count + "','" + website + "')";
				conn.update(sql);
			}
		}
		conn.closeConn();
		conn = null;
		movielist.clear();
	}
	
	public void referencevideo() {
		JDBCConnection conn = new JDBCConnection();
		for (int i = 0; i < videolist.size(); i++) {
			TextValue tv = (TextValue) videolist.get(i);
			String[] splits = tv.text.split("@@");
			if (splits != null && splits.length == 2) {
				String rowkey = splits[0];
				String tableName = splits[1];
				if (tableName.indexOf("moviedynamic") >= 0) {   //存videodynmaic.
					continue;
				}
				String date = tableName.substring(tableName.indexOf("2"));
				String count = tv.value.toString();
				String website = "iy";
				String sql = "";
				if (rowkey.contains("#vfrm=2-4-0-1+iy")) {
					sql = "insert into reference" + date
							+ "(rowkey,reference,website) values ('" + rowkey
							+ "','" + count + "','" + website + "')";

				} else {
				String	sqll="select rowkey from videoinfo where website='iy' and playrowkey="+"'"+rowkey+"'";
					ResultSet rs = conn.executeQuerySingle(sqll);
						try {
							ResultSetMetaData m = rs.getMetaData();
							while (rs.next()) {
								String rowkeyy = rs.getString(1);
								sql = "insert into reference" + date
								+ "(rowkey,reference,website) values ('"
								+ rowkeyy + "','" + count + "','" + website + "')";
					            // System.out.println("推荐");
							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
				if(!sql.equals("")) {
				conn.update(sql);
				}
			}
		}
		conn.closeConn();
		conn = null;
		videolist.clear();
	}
	

	public static int ConvertToInt(String str) {
		int value = -1;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return value;
	}

	public static long ConvertToLong(String str) {
		long value = -1;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Long.parseLong(str);
		} catch (Exception e) {
		}
		return value;
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

	public static double ConvertToDouble(String str) {
		double value = 0.0;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Double.parseDouble(str);
		} catch (Exception e) {
		}
		return value;
	}
}
