/*
 * 
 * @ 作者：韩嘉星
 *  
 *  @介绍： 解析源代码和动态数据
 *  
 *  @创建时间：2014.7.28
 *  
 *  @修改记录：
 *       
 *        
 * */
package ParserData.XunleiParserData;

import hbase.HBaseCRUD;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.TextValue;
import Utils.JDBCConnection;

public class XunleiParserData {
	public HBaseCRUD hbase;
	public JDBCConnection jdbconn;
	public static int a = 0;
	public String key;// 主键-
	public String url;// URL-
	public String date;// 时间
	public String category;// 类别-
	public String title;// 片名-
	public String subtitle;// 播放页标题-
	public String version;// 版本-
	public String showtype;// 版本-
	public String totle;// 播放量-
	public String director;// 导演-
	public String actor;// 演员-
	public String lenght;// 片长-
	public String genre;// 类型-
	public String rating;// 评分
	public String ratingNum;// 评分人数
	public String area;// 地区
	public String intro;// 剧情介绍
	public String year;// 年份
	// public String bitrate;//清晰度
	public String upNum;// 顶的人数-
	public String downNum;// 踩的人数-
	public String comment;// 影评
	public String commentNum;// 影评数-
	public String dcomment;// 短评
	public String dcommentNum;// 短评数-
	public String point;
	public String pointNum;// 看点数-
	
	public String movieid;

	public XunleiParserData(HBaseCRUD hbase, JDBCConnection jdbconn) {
		this.hbase = hbase;
		this.jdbconn = jdbconn;
	}

	public void resolveData(String dataContent) {
		String videoKey = null;
		String videoURL = null;
		String videoDate = null;
		String videoCatagory = null;
		String videoSource = null;
		String videoSubtitle = null;
		String videoLike = null;
		String videoPoint = null;
		String videoComment = null;
		String videoDcomment = null;
		String detailData = null;
		// String videoRating = null; detailData可获得评分

		int keyStart = dataContent.indexOf("<*RK->");
		int keyEnd = dataContent.indexOf("<-RK*>");

		int urlStart = dataContent.indexOf("<*UL->");
		int urlEnd = dataContent.indexOf("<-UL*>");

		int dateStart = dataContent.indexOf("<*DT->");
		int dateEnd = dataContent.indexOf("<-DT*>");

		int catagoryStart = dataContent.indexOf("<*TY->");
		int catagoryEnd = dataContent.indexOf("<-TY*>");

		int soureStart = dataContent.indexOf("<*SC->");
		int soureEnd = dataContent.indexOf("<-SC*>");

		int subtitleStart = dataContent.indexOf("<*ST->");
		int subtitleEnd = dataContent.indexOf("<-ST*>");

		int likeStart = dataContent.indexOf("<*LK->");
		int likeEnd = dataContent.indexOf("<-LK*>");

		int pointStart = dataContent.indexOf("<*PT->");
		int pointEnd = dataContent.indexOf("<-PT*>");

		int commentStart = dataContent.indexOf("<*CMCN->");
		int commentEnd = dataContent.indexOf("<-CMCN*>");

		int dcommentStart = dataContent.indexOf("<*DCM->");
		int dcommentEnd = dataContent.indexOf("<-DCM*>");

		// int ratingStart = dataContent.indexOf("<*RT->");
		// int ratingEnd = dataContent.indexOf("<-RT*>");

		int detailDataStart = dataContent.indexOf("<*MD->");
		int detailDataEnd = dataContent.indexOf("<-MD*>");

		if (keyStart > -1 && keyEnd > keyStart) {
			videoKey = dataContent.substring(keyStart + 6, keyEnd);
		} else {
			videoKey = null;
		}

		if (urlStart > -1 && urlEnd > urlStart) {
			videoURL = dataContent.substring(urlStart + 6, urlEnd);
		} else {
			videoURL = null;
		}

		if (dateStart > -1 && dateEnd > dateStart) {
			videoDate = dataContent.substring(dateStart + 6, dateEnd);
		} else {
			videoDate = null;
		}

		if (catagoryStart > -1 && catagoryEnd > catagoryStart) {
			videoCatagory = dataContent.substring(catagoryStart + 6,
					catagoryEnd);
		} else {
			videoCatagory = null;
		}

		if (soureStart > -1 && soureEnd > soureStart) {
			videoSource = dataContent.substring(soureStart + 6, soureEnd);
		} else {
			videoSource = null;
		}

		if (subtitleStart > -1 && subtitleEnd > subtitleStart) {
			videoSubtitle = dataContent.substring(subtitleStart + 6,
					subtitleEnd);
		} else {
			videoSubtitle = null;
		}

		if (likeStart > -1 && likeEnd > likeStart) {
			videoLike = dataContent.substring(likeStart + 6, likeEnd);
		} else {
			videoLike = null;
		}

		if (pointStart > -1 && pointEnd > pointStart) {
			videoPoint = dataContent.substring(pointStart + 6, pointEnd);
		} else {
			videoPoint = null;
		}

		if (commentStart > -1 && commentEnd > commentStart) {
			videoComment = dataContent.substring(commentStart + 8, commentEnd);
		} else {
			videoComment = null;
		}

		if (dcommentStart > -1 && dcommentEnd > dcommentStart) {
			videoDcomment = dataContent.substring(dcommentStart + 6,
					dcommentEnd);
		} else {
			videoDcomment = null;
		}

		// if(ratingStart >-1){
		// videoRating = dataContent.substring(ratingStart + 6, ratingEnd);
		// }
		// else{
		// videoRating = null;
		// }

		if (detailDataStart > -1 && detailDataEnd > detailDataStart) {
			detailData = dataContent.substring(detailDataStart + 6,
					detailDataEnd);
		} else {
			detailData = null;
		}

		extractKey(videoKey);
		extractBaseData(detailData);
		extractDetail(detailData);
		extractURL(videoURL);
		extractCatagory(videoCatagory);
		extractDate(videoDate);
		extractSubtitle(videoSubtitle);
		extractSource(videoSource);
		extractLike(videoLike);
		extractComment(videoComment);
		extractPoint(videoPoint);
		save();
	}

	/*
	 * 获取URL
	 */
	public void extractURL(String videoURL) {
		if (videoURL != null) {
			url = videoURL;
		} else {
			url = "-1";
		}
	}

	/*
	 * 获取抓取时间
	 */
	public void extractDate(String videoDate) {
		if (videoDate != null) {
			date = videoDate;
		} else {
			date = "-1";
		}
	}

	/*
	 * 获取抓取时间
	 */
	public void extractCatagory(String videoCatagory) {
		if (videoCatagory != null) {
			category = videoCatagory;
		} else {
			category = "-1";
		}
	}

	/*
	 * 获取键值（movieid - subid）
	 */
	public void extractKey(String videoKey) {
		if (videoKey != null) {
			key = videoKey;
			int index = key.indexOf("-");
			if (index > 0)
				movieid = key.substring(0, index);
			else
				movieid = null;
		} else {
			key = null;
			movieid = null;
		}
	}

	/*
	 * 获取播放页标题
	 */
	public void extractSubtitle(String videoSubtitle) {
		if (videoSubtitle != null) {
			subtitle = videoSubtitle;
			if (subtitle.contains("全集"))
				showtype = "正片";
			else if (subtitle.contains("预告片") || subtitle.contains("宣传片")) {
				showtype = "预告片";
			} else if (subtitle.contains("MV") || subtitle.contains("mv")) {
				showtype = "MV";
			} else if (subtitle.contains("花絮") || subtitle.contains("删减片段")
					|| subtitle.contains("制作特辑")) {
				showtype = "花絮";
			}

			else {
				if (subtitle.length() > 15) {
					showtype = "预告片";
				} else
					showtype = "正片";
			}
		} else {
			subtitle = "-1";
		}
	}

	/*
	 * 获取片长和短评数
	 */
	public void extractSource(String videoSource) {
		if (videoSource != null) {
			int lenghtStart = -1;
			int lenghtEnd = -1;
			int dcmentNumStart = -1;
			int dcmentNumEnd = -1;
			int totleStart = -1;
			int totleEnd = -1;
			lenghtStart = videoSource.indexOf("片长:<span>");
			lenghtEnd = videoSource.indexOf("分钟</span>");
			if (lenghtEnd > lenghtStart + 9) {
				lenght = videoSource.substring(lenghtStart + 9, lenghtEnd);
			} else {
				lenght = "-1";
			}

			dcmentNumStart = videoSource.lastIndexOf("\">全部");
			dcmentNumEnd = videoSource.indexOf("条短评</a>");
			if (dcmentNumStart + 4 < dcmentNumEnd) {
				dcommentNum = videoSource.substring(dcmentNumStart + 4,
						dcmentNumEnd);
			} else {
				dcommentNum = "-1";

			}

			totleStart = videoSource.indexOf("G_PLAY_VV");
			if (totleStart > 0) {
				String detailDataTemp = videoSource.substring(totleStart);
				totleEnd = detailDataTemp.indexOf("</script>");

				if (totleEnd > 0) {
					totle = getNumberOfStr(detailDataTemp
							.substring(0, totleEnd));
				} else {
					totle = "-1";
				}
			} else {
				totle = "-1";
				// System.out.println(++a + "  " +subtitle + "");
			}

		} else {
			lenght = "-1";
			dcommentNum = "-1";
			totle = "-1";
		}
	}

	/*
	 * 获取顶和踩数
	 */
	public void extractLike(String videoLike) {
		if (videoLike != null) {
			int upStart = -1;
			int uptEnd = -1;
			int downEnd = -1;
			upStart = videoLike.indexOf("up");
			uptEnd = videoLike.indexOf("down");
			downEnd = videoLike.indexOf("likeStatus");
			if (upStart < uptEnd && uptEnd > 0 && downEnd > uptEnd) {
				upNum = getNumberOfStr(videoLike.substring(upStart, uptEnd));
				downNum = getNumberOfStr(videoLike.substring(uptEnd, downEnd));
			} else {
				upNum = "-1";
				downNum = "-1";
			}
		} else {
			upNum = "-1";
			downNum = "-1";
		}
	}

	/*
	 * 获取影片基本信息
	 */
	public void extractBaseData(String detailData) {
		if (detailData != null) {

			int versionStart = -1;

			int directorStart = -1;
			int directorEnd = -1;
			int actorStart = -1;
			int actorEnd = -1;
			int genreStart = -1;
			int genreEnd = -1;
			int areaStart = -1;
			// int areaeEnd = -1;
			int introStart = -1;
			int introEnd = -1;

			// versionStart = detailData.indexOf(".version='");
			// if(versionStart > 0){
			// String detailDataTemp = detailData.substring(versionStart);
			// int versionEnd = detailDataTemp.indexOf("';");
			//
			// if(versionEnd > 10){
			// version = detailDataTemp.substring(10, versionEnd);
			// }
			// else{
			// version = "无";
			// }
			// }
			// else{
			// version = "无";
			// }

			directorStart = detailData.indexOf(".director=");
			if (directorStart > 0) {
				String detailDataTemp = detailData.substring(directorStart);
				directorStart = detailDataTemp.indexOf(",name:'");
				directorEnd = detailDataTemp.indexOf("',url:'");
				if (directorStart < directorEnd && directorEnd > 0) {
					if (directorStart + 7 != directorEnd
							&& directorStart + 7 < directorEnd) {
						director = detailDataTemp.substring(directorStart + 7,
								directorEnd);
						detailDataTemp = detailDataTemp
								.substring(directorEnd + 7);
						int urlEnd = detailDataTemp.indexOf("'}");
						director += "@url:"
								+ detailDataTemp.substring(0, urlEnd) + "@";
						detailDataTemp = detailDataTemp.substring(urlEnd);
						while (detailDataTemp.indexOf(",") == 2) {
							directorStart = detailDataTemp.indexOf(",name:'");
							if (directorStart > 0) {
								detailDataTemp = detailDataTemp
										.substring(directorStart);
								directorEnd = detailDataTemp.indexOf("',url:'");
								if (directorEnd > directorStart + 7) {
									director += detailDataTemp.substring(
											directorStart + 7, directorEnd);
								}
								detailDataTemp = detailDataTemp
										.substring(directorEnd + 7);
								urlEnd = detailDataTemp.indexOf("'}");
								director += "@url:"
										+ detailDataTemp.substring(0, urlEnd)
										+ "@";
								detailDataTemp = detailDataTemp
										.substring(urlEnd);
							}
						}
					} else {
						director = "无";
					}

				} else {
					director = "无";
				}
			} else {
				director = "无";
			}

			actorStart = detailData.indexOf(".actor=");
			if (actorStart > 0) {
				String detailDataTemp = detailData.substring(actorStart);
				actorStart = detailDataTemp.indexOf(",name:'");
				actorEnd = detailDataTemp.indexOf("',url:'");
				if (actorStart < actorEnd) {
					if (actorStart + 7 != actorEnd) {
						actor = detailDataTemp.substring(actorStart + 7,
								actorEnd);
						detailDataTemp = detailDataTemp.substring(actorEnd + 7);
						int urlEnd = detailDataTemp.indexOf("'}");
						actor += "@url:" + detailDataTemp.substring(0, urlEnd)
								+ "@";
						detailDataTemp = detailDataTemp.substring(urlEnd);

						while (detailDataTemp.indexOf(",") == 2) {
							actorStart = detailDataTemp.indexOf(",name:'");
							if (actorStart > 0) {
								// detailDataTemp =
								// detailDataTemp.substring(actorStart);
								actorEnd = detailDataTemp.indexOf("',url:'");
								if (actorStart < actorEnd && actorEnd > 0) {
									actor += detailDataTemp.substring(
											actorStart + 7, actorEnd);
								}
								detailDataTemp = detailDataTemp
										.substring(actorEnd + 7);
								urlEnd = detailDataTemp.indexOf("'}");
								actor += "@url:"
										+ detailDataTemp.substring(0, urlEnd)
										+ "@";
								detailDataTemp = detailDataTemp
										.substring(urlEnd);
							}
						}
					}

					else {
						actor = "无";
					}
				} else {
					actor = "无";
				}
			} else {
				actor = "无";
			}

			genreStart = detailData.indexOf(".genre=");
			if (genreStart > 0) {
				String detailDataTemp = detailData.substring(genreStart);
				genreStart = detailDataTemp.indexOf(",name:'");
				genreEnd = detailDataTemp.indexOf("',url:'");
				if (genreStart + 7 < genreEnd) {
					genre = detailDataTemp.substring(genreStart + 7, genreEnd);
					detailDataTemp = detailDataTemp.substring(genreEnd + 7);
					int urlEnd = detailDataTemp.indexOf("'}");
					// genre += "@url:" + detailDataTemp.substring(0, urlEnd) +
					// "@";
					// genre += "@";
					detailDataTemp = detailDataTemp.substring(urlEnd);

					while (detailDataTemp.indexOf(",") == 2) {
						genre += "@";
						genreStart = detailDataTemp.indexOf(",name:'");
						if (genreStart > 0) {
							// detailDataTemp =
							// detailDataTemp.substring(genreStart);
							genreEnd = detailDataTemp.indexOf("',url:'");
							if (genreStart + 7 < genreEnd) {
								genre += detailDataTemp.substring(
										genreStart + 7, genreEnd);
							}
							detailDataTemp = detailDataTemp
									.substring(genreEnd + 7);
							urlEnd = detailDataTemp.indexOf("'}");
							// genre += "@url:" + detailDataTemp.substring(0,
							// urlEnd) + "@";
							// genre += "@";
							detailDataTemp = detailDataTemp.substring(urlEnd);
						}
					}

				} else {
					genre = "无";
				}
			} else {
				genre = "无";
			}

			areaStart = detailData.indexOf(".area=");
			if (areaStart > 0) {
				String detailDataTemp = detailData.substring(areaStart);
				areaStart = detailDataTemp.indexOf(",name:'");
				detailDataTemp = detailDataTemp.substring(areaStart);
				int areaEnd = detailDataTemp.indexOf("',url:'");
				if (areaEnd > 7) {
					area = detailDataTemp.substring(7, areaEnd);
					int urlEnd = detailDataTemp.indexOf("'}");
					// area += "@url:" + detailDataTemp.substring(areaEnd + 7,
					// urlEnd) + "@";
				} else {
					area = "无";
				}
			}
			introStart = detailData.indexOf(".intro='");
			if (introStart > 0) {
				String detailDataTemp = detailData.substring(introStart);
				introEnd = detailDataTemp.indexOf("';");
				if (introEnd > 8) {
					intro = detailDataTemp.substring(8, introEnd);
				} else {
					intro = "-1";
				}
			}

		} else {

			// version = "无";
			director = "无";
			actor = "无";
			genre = "无";
			area = "无";
			intro = "无";

		}
	}

	public void extractDetail(String detailData) {
		if (detailData != null) {
			int titleStart = -1;
			int titleEnd = -1;
			int ratingStart = -1;
			int ratingNumStart = -1;
			int yearStart = -1;
			int yearEnd = -1;
			// int totleStart = -1;
			// int totleEnd = -1;

			titleStart = detailData.indexOf(".title='");
			if (titleStart > 0) {
				String detailDataTemp = detailData.substring(titleStart);

				titleEnd = detailDataTemp.indexOf("';");

				if (titleEnd > 8) {
					title = detailDataTemp.substring(8, titleEnd);
				} else {
					title = "-1";
				}
			} else {
				title = "-1";
			}

			ratingStart = detailData.indexOf(".rating='");
			if (ratingStart > 0) {
				String detailDataTemp = detailData.substring(ratingStart);
				int ratingEnd = detailDataTemp.indexOf("';");
				if (ratingEnd > 9) {
					rating = detailDataTemp.substring(9, ratingEnd);
				} else {
					rating = "-1";
				}
			} else {
				rating = "-1";
			}

			ratingNumStart = detailData.indexOf(".rating_num=");
			if (ratingNumStart > 0) {
				String detailDataTemp = detailData.substring(ratingNumStart);
				int ratingNumEnd = detailDataTemp.indexOf(";");
				if (ratingNumEnd > 12) {
					ratingNum = detailDataTemp.substring(12, ratingNumEnd);
				} else {
					ratingNum = "-1";
				}
			}

			yearStart = detailData.indexOf(".year=");
			if (yearStart > 0) {
				String detailDataTemp = detailData.substring(yearStart);
				yearStart = detailDataTemp.indexOf("name:'");
				detailDataTemp = detailDataTemp.substring(yearStart);
				yearEnd = detailDataTemp.indexOf("',url:'");
				if (yearStart < yearEnd && yearEnd > 6) {
					year = detailDataTemp.substring(6, yearEnd);
					int urlEnd = detailDataTemp.indexOf("'}");
					// year += "@url:" + detailDataTemp.substring(0, urlEnd) +
					// "@";
				} else {
					year = "-1";
				}
			}

		} else {
			rating = "-1";
			ratingNum = "-1";
			year = "-1";

		}

	}

	/*
	 * 获取 看点数（后期和获取评论内容）
	 */
	public void extractPoint(String videoPoint) {
		if (videoPoint != null) {
			int pointNumStart = -1;
			pointNumStart = videoPoint.indexOf("count&quot;:");
			int pointEnd = -1;
			pointEnd = videoPoint.lastIndexOf("}");

			if (pointNumStart + 12 < pointEnd) {
				pointNum = videoPoint.substring(pointNumStart + 12, pointEnd);
			} else {
				pointNum = "0";
			}
		} else {
			pointNum = "0";
		}
	}

	/*
	 * 获取 短评内容（后期实现）
	 */
	public void extractDcomment(String videoDcomment) {
		// if(videoComment != null){
		// int commentNumStart = videoComment.indexOf("<em class=\"num\">");
		// int commentNumEnd = videoComment.indexOf("</em></h1>");
		//
		// if(commentNumStart > commentNumEnd){
		// commentNum = getNumberOfStr(videoComment.substring(commentNumStart,
		// commentNumEnd));
		// }
		//
		// }
	}

	/*
	 * 获取影评数（后期和获取评论内容）
	 */
	public void extractComment(String videoComment) {
		if (videoComment != null) {
			int commentNumStart = -1;
			int commentNumEnd = -1;
			commentNumStart = videoComment.indexOf("<em class=\"num\">");
			commentNumEnd = videoComment.indexOf("</em></h1>");

			if (commentNumStart > 0 && commentNumStart < commentNumEnd) {
				commentNum = getNumberOfStr(videoComment.substring(
						commentNumStart, commentNumEnd));
			} else {
				commentNum = "-1";
			}
		} else {
			commentNum = "-1";
		}
	}

	public void save() {
		String rowkey = movieid + "+xl";
		if (key == null || movieid == null ) {
			jdbconn.log("韩嘉星", "", 1, "xl", url, "rowkey roor", 2);
			return;
		}
		if (title == "-1" || subtitle == "-1") {
			jdbconn.log("韩嘉星", rowkey, 1, "xl", url, "title eroor", 2);
			return;
		}
		if (category == "-1") {
			jdbconn.log("韩嘉星", rowkey, 1, "xl", url, "category eroor", 2);
			return;
		}
		if (date == "-1") {
			jdbconn.log("韩嘉星", rowkey, 1, "xl", url, "date eroor", 2);
			return;
		}
		if (totle == "-1") {
			jdbconn.log("韩嘉星", rowkey, 1, "xl", url, "totle eroor", 2);
			return;
		}
		if (infoIsExist(rowkey, "movie") == 0) {
			movieinfoToHBase();
			movieinfoToMySQL();
		}
		
		if (moviedynamicIsExist(rowkey) == 0) {
			moviedynamicToHBase();
			moviedynamicToMySQL();
		}
		
		if (infoIsExist(key, "video") == 0) {
			videoinfoToHBase();
			videoinfoToMySQL();
		}	
		videodynamicToHBase();
		videodynamicToMySQL();
	}

	public void movieinfoToHBase() {
		String rowkey = movieid + "+xl";
		String[] rows;
		String[] colfams;
		String[] quals;
		String[] values;

		rows = new String[] { rowkey, rowkey, rowkey, rowkey, rowkey, rowkey,
				rowkey, rowkey, rowkey, rowkey, rowkey, rowkey };
		colfams = new String[] { "R", "R", "R", "B", "B", "B", "B", "B", "B",
				"B", "B", "B" };
		quals = new String[] { "info", "year", "website", "name", "area",
				"type", "director", "mainactor", "duration", "category",
				"summarize", "url" };
		values = new String[] { key, year, "xl", title, area, genre, director,
				actor, lenght, category, intro, url };
		try {
			hbase.putRows("movieinfo", rows, colfams, quals, values);
			hbase.putRows("movieinfobak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void moviedynamicToHBase() {

		String rowkey = movieid + "+xl" + "+n" + "+" + date;
		String[] rows;
		String[] colfams;
		String[] quals;
		String[] values;

		rows = new String[] { rowkey, rowkey, rowkey, rowkey, rowkey, rowkey,
				rowkey, rowkey, rowkey, rowkey, rowkey };
		colfams = new String[] { "R", "R", "R", "R", "R", "C", "C", "C", "C",
				"C", "C" };
		quals = new String[] { "inforowkey", "year", "website", "flag",
				"timestamp", "name", "score", "sumplaycount", "scoreuum",
				"comment", "dcoment" };
		values = new String[] { key, year, "xl", "n", date, title, rating,
				totle, ratingNum, commentNum, dcommentNum };
		try {
			hbase.putRows("moviedynamic", rows, colfams, quals, values);
			hbase.putRows("moviedynamicbak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void videoinfoToHBase() {
		String rowkey = movieid + key + "+xl";

		String[] rows;
		String[] colfams;
		String[] quals;
		String[] values;

		rows = new String[] { rowkey, rowkey, rowkey, rowkey, rowkey, rowkey };
		colfams = new String[] { "R", "R", "R", "B", "B", "B" };
		quals = new String[] { "inforowkey", "playrowkey", "website", "name",
				"url", "showtype" };
		values = new String[] { movieid, key, "xl", title + ":" + subtitle,
				url, showtype };
		try {
			hbase.putRows("videoinfo", rows, colfams, quals, values);
			hbase.putRows("videoinfobak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void videodynamicToHBase() {

		String rowkey = movieid + key + "+xl" + "+n" + "+" + date;
		String[] rows;
		String[] colfams;
		String[] quals;
		String[] values;

		rows = new String[] { rowkey, rowkey, rowkey, rowkey, rowkey, rowkey,
				rowkey };
		colfams = new String[] { "R", "R", "R", "R", "R", "C", "C" };
		quals = new String[] { "inforowkey", "playrowkey", "website", "flag",
				"timestamp", "updown", "point" };
		values = new String[] { movieid, key, "xl", "n", date,
				upNum + "@" + downNum, pointNum };
		try {
			hbase.putRows("videodynamic", rows, colfams, quals, values);
			hbase.putRows("videodynamicbak", rows, colfams, quals, values);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void movieinfoToMySQL() {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(date)));
		
		String[] directorSplits = null;
		if (director != "无") {
			int index = director.indexOf("@");
			if (index >= 0)
				directorSplits = director.split("@");
		}

		String[] mainactorSplits = null;
		if (actor != "无") {
			int index = actor.indexOf("@");
			if (index >= 0)
				mainactorSplits = actor.split("@");
		}

		String[] typeSplits = null;
		if (genre != "无") {
			typeSplits = genre.split("@");
		}

		ArrayList<TextValue> values = new ArrayList<TextValue>();

		TextValue yeartv = new TextValue();
		yeartv.text = "year";
		yeartv.value = year;
		values.add(yeartv);

		TextValue pricetv = new TextValue();
		pricetv.text = "price";
		pricetv.value = "-1";
		values.add(pricetv);

		TextValue timetv = new TextValue();
		timetv.text = "time";
		timetv.value = -1;
		values.add(timetv);

		TextValue ytimetv = new TextValue();
		ytimetv.text = "ytime";
		ytimetv.value = -1;
		values.add(ytimetv);

		TextValue crawltimetv = new TextValue();
		crawltimetv.text = "crawltime";
		crawltimetv.value = sd;
		values.add(crawltimetv);

		TextValue duratv = new TextValue();
		duratv.text = "duration";
		duratv.value = lenght;
		values.add(duratv);

		TextValue summarizetv = new TextValue();
		summarizetv.text = "summarize";
		summarizetv.value = intro;
		if (intro.equals("无") == false)
			values.add(summarizetv);

		TextValue areatv = new TextValue();
		areatv.text = "area";
		areatv.value = area;
		if (area.equals("无") == false)
			values.add(areatv);

		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = movieid + "+xl";
		values.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "moviename";
		nametv.value = title;
		values.add(nametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "xl";
		values.add(namewebsite);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		//
		if (category.equals("电影"))
			categorytv.value = "movie";
		else if (category.equals("电视剧"))
			categorytv.value = "tv";
		else if (category.equals("动漫"))
			categorytv.value = "dongman";
		else if (category.equals("综艺"))
			categorytv.value = "zongyi";

		values.add(categorytv);

		for (int i = 0; i < typeSplits.length; i++) {
			TextValue typetv = new TextValue();
			typetv.text = "type" + (i + 1);
			typetv.value = typeSplits[i];
			values.add(typetv);
			if (i == 2)
				break;
		}

		//
		if (directorSplits != null) {
			for (int i = 0; i < directorSplits.length / 2; i++) {
				TextValue directortv = new TextValue();
				directortv.text = "director" + (i + 1);
				directortv.value = directorSplits[i * 2];
				values.add(directortv);
				if (i == 2)
					break;
			}
		}

		//
		if (mainactorSplits != null) {
			for (int i = 0; i < mainactorSplits.length / 2; i++) {
				TextValue actortv = new TextValue();
				actortv.text = "mainactor" + (i + 1);
				actortv.value = mainactorSplits[i * 2];
				values.add(actortv);
				if (i == 2)
					break;
			}
		}
		jdbconn.insert(values, "movieinfo");
	}

	public void moviedynamicToMySQL() {

		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = movieid + "+xl";
		values.add(rowkeytv);

		TextValue scoretv = new TextValue();
		scoretv.text = "score";
		scoretv.value = rating;
		values.add(scoretv);

		TextValue categorytv = new TextValue();
		categorytv.text = "category";
		//
		if (category.equals("电影"))
			categorytv.value = "movie";
		else if (category.equals("电视剧"))
			categorytv.value = "tv";
		else if (category.equals("动漫"))
			categorytv.value = "dongman";
		else if (category.equals("综艺"))
			categorytv.value = "zongyi";
		values.add(categorytv);

		TextValue sumPlayCounttv = new TextValue();
		sumPlayCounttv.text = "sumPlayCount";
		sumPlayCounttv.value = totle;
		values.add(sumPlayCounttv);

		TextValue uptv = new TextValue();
		uptv.text = "up";
		uptv.value = -1;
		values.add(uptv);

		TextValue downtv = new TextValue();
		downtv.text = "down";
		downtv.value = -1;
		values.add(downtv);

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

		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// n 2:y
		values.add(flagtv);

		TextValue commenttv = new TextValue();
		commenttv.text = "comment";
		commenttv.value = commentNum;// n 2:y
		values.add(commenttv);

		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = date.substring(0,date.length()-3);
		values.add(timestamptv);

		TextValue movienametv = new TextValue();
		movienametv.text = "movieName";
		movienametv.value = title;
		values.add(movienametv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "xl";
		values.add(namewebsite);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(date)));

		jdbconn.insert(values, "moviedynamic" + sd);

	}

	public void videoinfoToMySQL() {
		String timeString = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		timeString = sdf.format(new Date());

		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = movieid + "+xl";
		values.add(rowkeytv);

		TextValue nametv = new TextValue();
		nametv.text = "name";
		nametv.value = title + ":" + subtitle;
		values.add(nametv);

		TextValue timetv = new TextValue();
		timetv.text = "crawltime";
		timetv.value = timeString;
		values.add(timetv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "xl";
		values.add(namewebsite);

		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = movieid;
		values.add(inforowkeytv);

		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = key;
		values.add(playrowkeytv);

		if (showtype == null || showtype == "" || showtype.isEmpty()) {
			if (subtitle.contains("全集")) {
				showtype = "正片";
			} else if (subtitle.contains("MV") || subtitle.contains("mv")) {
				showtype = "MV";
			} else if (subtitle.contains("花絮") || subtitle.contains("删减片段")
					|| subtitle.contains("制作特辑")) {
				showtype = "花絮";
			} else {
				if (subtitle.length() > 20)
					showtype = "预告片";
				else
					showtype = "正片";
			}
		}

		TextValue showtypetv = new TextValue();
		showtypetv.text = "showtype";
		showtypetv.value = showtype;
		values.add(showtypetv);

		jdbconn.insert(values, "videoinfo");
	}

	public void videodynamicToMySQL() {

		ArrayList<TextValue> values = new ArrayList<TextValue>();
		TextValue rowkeytv = new TextValue();
		rowkeytv.text = "rowkey";
		rowkeytv.value = movieid + "+xl";
		values.add(rowkeytv);

		TextValue inforowkeytv = new TextValue();
		inforowkeytv.text = "inforowkey";
		inforowkeytv.value = movieid;
		values.add(inforowkeytv);

		TextValue playrowkeytv = new TextValue();
		playrowkeytv.text = "playrowkey";
		playrowkeytv.value = key;
		values.add(playrowkeytv);

		TextValue uptv = new TextValue();
		uptv.text = "up";
		uptv.value = upNum;
		values.add(uptv);

		TextValue downtv = new TextValue();
		downtv.text = "down";
		downtv.value = downNum;
		values.add(downtv);

		TextValue sumplaycounttv = new TextValue();
		sumplaycounttv.text = "sumplaycount";
		sumplaycounttv.value = -1;
		values.add(sumplaycounttv);

		TextValue commenttv = new TextValue();
		commenttv.text = "comment";
		commenttv.value = -1;
		values.add(commenttv);

		TextValue collecttv = new TextValue();
		collecttv.text = "collect";
		collecttv.value = -1;
		values.add(collecttv);

		TextValue outsidetv = new TextValue();
		outsidetv.text = "outside";
		outsidetv.value = -1;
		values.add(outsidetv);

		TextValue flagtv = new TextValue();
		flagtv.text = "flag";
		flagtv.value = 1;// n 2:y
		values.add(flagtv);

		TextValue timestamptv = new TextValue();
		timestamptv.text = "timestamp";
		timestamptv.value = date.substring(0,date.length()-3);
		values.add(timestamptv);

		TextValue namewebsite = new TextValue();
		namewebsite.text = "website";
		namewebsite.value = "xl";
		values.add(namewebsite);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(date)));
		jdbconn.insert(values, "videodynamic" + sd);

	}

	/*
	 * 获取字符串中的数字
	 */
	public String getNumberOfStr(String str) {
		Pattern pattern = Pattern.compile("[^0-9]");
		Matcher matcher = pattern.matcher(str);
		String count = matcher.replaceAll("");
		return count;
	}

	
	public int infoIsExist(String rowkey, String tabletype) {
		int count = 0;
		count = jdbconn.executeQueryCount("select count(*) as count from " + tabletype
				+ "info where rowkey = '" + rowkey + "'");
		return count;
	}
	public int moviedynamicIsExist(String rowkey) {
		int count = 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String sd = sdf.format(new Date(Long.parseLong(date)));
		count = jdbconn.executeQueryCount("select count(*) as count from moviedynamic" + sd 
				+ " where rowkey = '" + rowkey + "' and timestamp = '" + date.substring(0,date.length()-3)+ "'");
		return count;
	}
}
