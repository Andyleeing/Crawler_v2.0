/*
 * 
 * @ 作者：韩嘉星
 *  
 *  @介绍：抓取源代码和动态数据
 *  
 *  @创建时间：2014.7.28
 *  
 *  @修改记录：
 * 
 *        
 * */
package DataCrawler.XunleiDataCrawler;



import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import Utils.JDBCConnection;
import DataCrawler.CrawlerThread;

public class XunleiDataCrawler{
	public String topid = null;
	public String movieid = null;
	public String subid = null;
	public String url = null;
	public String sourcedata = null;
	private long time;
	private String basedata;
	public JDBCConnection jdbc;
	public XunleiDataCrawler(long time,JDBCConnection jdbc) {
		this.time = time;
		this.jdbc= jdbc;
	}
	
	public int crawler(String url) {
		int flag = 1; //1：作为抓取结果返回标识      -1： 抓取失败
		int index = url.indexOf("@");
		String videourl = url.substring(0, index);
		String title = url.substring(index + 1);
		infoCrawler(videourl.substring(7), title);
		return flag;
	}
	
	public int infoCrawler(String subMovieUrl, String baseInfo) {
		url = subMovieUrl;
		String fatherUrl = getFatherUrl(subMovieUrl);
		topid = getTopId(fatherUrl);
		movieid = getMovieId(fatherUrl);
		commentSearch(subMovieUrl, movieid);
		movieDataSearch(subMovieUrl, topid, movieid);	
		if (basedata.indexOf("<*MD->无<-MD*>") < 0) {
			subid = getMovieId(subMovieUrl);
			DocumetContent docContent = new DocumetContent();
			Document doc = docContent.getDocument(subMovieUrl);
			if (doc != null) {
				if (doc.toString().length() > 200) {
					sourcedata = "<*SC->" + doc.toString() + "<-SC*>";
					likeSearch(subMovieUrl, movieid, subid);
					saveData(sourcedata + baseInfo + basedata, movieid, subid,
						subMovieUrl);
				}
				else {
					jdbc.log("韩嘉星", movieid  + "+xl", 1, "xl", url, "invalid url", 1);
					return -1;
				}
					
			}
			else {
				jdbc.log("韩嘉星", movieid  + "+xl", 1, "xl", url, "no content", 1);
				return -1;
			}
		}
		else {
			jdbc.log("韩嘉星", movieid  + "+xl", 1, "xl", url, "no content", 1);
			return -1;
		}
		return 1;

	}
	/*
	 * 
	 * 功能：抓取迅雷看看电影的喜好内容（顶、踩）
	 */
	public void likeSearch(String strUrl, String movieId, String subId) {
		try {
			if (movieId != null && subId != null) {
				String likeUrl = "http://api.t.kankan.com/like.json?a=getLikeStutas&movieid="
						+ movieId + "&" + "subid=" + subId;
				DocumetContent docContent = new DocumetContent();
				Document doc = docContent.getDocument(likeUrl);
				if (doc != null) {
					sourcedata += "<*LK->" + doc.toString() + "<-LK*>";
				} else {
					sourcedata += "<*LK->无<-LK*>";
					// System.out.println("无顶踩"+strUrl);
				}
			}
		}// try
		catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			// System.out.println("likeSearch" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 功能：抓取迅雷看看电影的看点
	 */
	public void pointSearch(String strUrl, String subId) {
		try {
			if (subId != null) {
				String pointUrl = "http://point.api.t.kankan.com/point.json?a=show&subid="
						+ subId + "&start=1&end=900&jsobj=vpList";
				DocumetContent docContent = new DocumetContent();
				Document doc = docContent.getDocument(pointUrl);
				if (doc != null) {
					sourcedata += "<*PT->" + doc.toString() + "<-PT*>";
				} else {
					sourcedata += "<*PT->无<-PT*>";
					// System.out.println("无看点"+strUrl);
				}
			}
		}// try
		catch (Exception e) {
			// LogForKankan.logForSpider.error("------------"+strUrl+"----------------");
			// System.out.println("pointSearch" + strUrl);
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 功能：抓取评论
	 */
	public void commentSearch(String strUrl, String movieId) {
		try {
			if (movieId != null) {
				String commentUrl = "http://t.kankan.com/app/movie_comments?id="
						+ movieId;
				DocumetContent docContent = new DocumetContent();
				Document doc = docContent.getDocument(commentUrl);
				Element commentCountText = null;

				if (doc != null) {
					commentCountText = doc.getElementsByClass("l_tit").first();
					if (commentCountText != null)
						basedata = "<*CMCN->" + commentCountText.toString()
								+ "<-CMCN*>";
					else {
						basedata = "<*CMCN>无<-CMCN*>";
						// System.out.println("无影评数"+strUrl);
					}
				}
				// else
				// out.write(("<*CMSC>无<-CMSC*>").getBytes());

			}
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 功能：抓短评内容（短评数在源代码中可以找到）
	 */
	public void dcommentSearch(String strUrl, String movieId) {
		try {

			if (movieId != null) {
				String dcommentUrl = "http://t.kankan.com/app/dcomments/"
						+ movieId + ".html";
				DocumetContent docContent = new DocumetContent();
				Document doc = docContent.getDocument(dcommentUrl);
				if (doc != null)
					basedata += "<*DCM->" + doc.toString() + "<-DCM*>";

				else {
					basedata += "<*DCM->无<-DCM*>";
					// System.out.println("无短评内容"+strUrl);
				}
			}

		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * 功能：抓取基本信息（片长在源代码里可查）
	 */
	public void movieDataSearch(String strUrl, String topId, String movieId) {
		try {
			if (topId != null && movieId != null) {
				String movieDataUrl = "http://api.movie.kankan.com/vodjs/moviedata/"
						+ topId + "/" + movieId + ".js";
				DocumetContent docContent = new DocumetContent();
				String doc = docContent.getJsContent(movieDataUrl);
				if (doc != null)
					basedata += "<*MD->" + doc + "<-MD*>";
				else {
					basedata += "<*MD->无<-MD*>";
					// System.out.println("无电影信息"+strUrl);
				}
			}
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * @功能：存储源文件到本地
	 */
	public void saveData(String sourceData, String movieId, String subId,
			String url) {
		String rowkey = movieId + "-" + subId;
		String content = "<*RK->" + rowkey + "<-RK*>" + "<*UL->" + url
				+ "<-UL*>" + "<*DT->" + time + "<-DT*>" + sourceData;
		CrawlerThread.saveData("xunlei ", content.replaceAll("\n", ""));
	}


	/*
	 * 
	 * @功能：获取迅雷看看电影的topId
	 * 
	 * @例子：http://vod.kankan.com/v/68/68821 中 68是它的movieId
	 * 当URL为http://vod.kankan.com/v/68/68821/277188时，即它的子URL，该方法
	 * 也可以获得子URL的movieId
	 */
	public String getTopId(String strUrl) {
		try {
			int sub1 = strUrl.lastIndexOf('/');
			String movieIdTemp = strUrl.substring(0, sub1);
			int sub2 = movieIdTemp.lastIndexOf('/');
			String movieId = movieIdTemp.substring(sub2 + 1);
			return movieId;
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * 
	 * @功能：获取movieId
	 * 
	 * @例子：http://vod.kankan.com/v/68/68821 中 68821是它的movieId，
	 * 277188为它的subId,当URL为http://vod.kankan.com/v/68/68821/277188
	 * 时，即它的子URL，该方法也可以获得子URL的subId
	 */
	public String getMovieId(String strUrl) {
		try {
			int sub1 = strUrl.lastIndexOf('/');
			String subIdTemp = strUrl.substring(sub1 + 1);
			int sub3 = subIdTemp.indexOf(".shtml");
			String subId = subIdTemp.substring(0, sub3);
			return subId;
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * 
	 * 获取strUrl的父URL
	 */
	public String getFatherUrl(String strUrl) {
		try {
			int sub1 = strUrl.lastIndexOf('/');
			String subIdTemp = strUrl.substring(0, sub1);
			return subIdTemp + ".shtml";
		}// try
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


}
