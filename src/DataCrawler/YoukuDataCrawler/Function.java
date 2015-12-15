package DataCrawler.YoukuDataCrawler;

import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.WebSite.Base.GlobalData;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

public class Function {
	public static int i = 100;
	public static String visitURL(String href) {
		String content = null;
		int count = 0;
		while(true) {
			content = ConnectioinFuction.readURL(href);
			if(content != null && content.length() <= 5) {
				try{
					int num = Integer.parseInt(content);
					if(num == 404)
						return content;
					else if(num == 503) {
						count++;
						try {
							Thread.sleep(i);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						if(count == 7) {
							return content;
						}
						continue;
					}
				} catch(Exception e) {
					
				}
			}
			if(content != null && !content.equals("")) {
				if(href.indexOf("www.youku.com/show_page") >= 0) {
					if(content.indexOf("</html>") >= 0)
						break;
					else {
						count++;
						if(count == 7) {
							//content = "";
							break;
						}
						try {
							Thread.sleep(i);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
				} else if(href.indexOf("v.youku.com/v_show/") >= 0) {
					if(content.indexOf("<body class=\"page_v\">") > 0)
						break;
					else {
						count++;
						if(count == 7) {
							break;
						}
						try {
							Thread.sleep(i);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
				} else if(href.indexOf("http://v.youku.com/QVideo/~ajax/getVideoPlayInfo") >= 0) {
					int index1 = content.indexOf("\"vv\":");
					if(index1 >= 0 && content.indexOf(",",index1) > index1)
						break;
					else {
						count++;
						if(count == 7) {
							break;
						}
						try {
							Thread.sleep(i);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
				}
				break;
			}
			count++;
			try {
				Thread.sleep(i);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(count == 7) {
				content = "";
				break;
			}
		}
		return content;
	}

	public static String[] Filter(String content,String tag,String key,String value,String parsertarget,String position,int index) {
		String[] datas = null;
		NodeFilter filter = null;
		if(key == null || value == null)
			filter = new TagNameFilter(tag);
		else filter = new AndFilter(new TagNameFilter(tag), new HasAttributeFilter(
				key, value));
		try {
			Parser parser = new Parser(content);
			NodeList movielist = parser.parse(filter);
			if (movielist.size() > 0)
				datas = new String[movielist.size()];
			for (int i = 0; i < movielist.size(); i++) {
				Node movie = movielist.elementAt(i);
				String extract_href = "";
				if (position != null && position.equals("child"))
					extract_href = movie.getChildren().elementAt(index)
							.getText();
				else if (position != null && position.equals("self"))
					extract_href = movie.getText();
				if(GlobalData.category!=null&&GlobalData.category.equals("star"))
						datas[i] = getDataForYoukuStar(extract_href,parsertarget);
				else {
					datas[i] = getData(extract_href,parsertarget);
				}
			}
		} catch (ParserException e1) {
		}
		return datas;
	}
	public static String getData(String extract_href,String parsertarget) {
		String href = "";
		int indexofhref = extract_href.indexOf(parsertarget);
		if(indexofhref < 0)
			return href;
		href = extract_href.substring(indexofhref);
		indexofhref = href.indexOf("\"") + 1;
		href = href.substring(indexofhref,
				href.indexOf("\"", indexofhref));
		return href;
	}
	public static String getDataForYoukuStar(String extract_href,String parsertarget) {
		String href = "";
		if(extract_href.indexOf("star_page") > 0) {
			int indexofhref = extract_href.indexOf(parsertarget);
			href = extract_href.substring(indexofhref);
			indexofhref = href.indexOf("\"") + 1;
			href = href.substring(indexofhref,
			href.indexOf("\"", indexofhref));
		}
		return href;
	}

}
