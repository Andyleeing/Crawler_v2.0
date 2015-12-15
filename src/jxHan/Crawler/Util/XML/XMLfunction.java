package jxHan.Crawler.Util.XML;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import jxHan.Crawler.Util.FileHandler;
import jxHan.Crawler.Util.Connection.ConnectioinFuction;
import jxHan.Crawler.Util.Log.ExceptionHandler;
import jxHan.Crawler.WebSite.Base.GlobalData;
import org.dom4j.Element;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.HasAttributeFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
public class XMLfunction {
	private static XMLfunction function = null;

	public static XMLfunction getXMLfunction() {
		if (function == null)
			function = new XMLfunction();
		return function;
	}

	public void BaseURL(Element e) {
		String BaseURL = e.getText();
		GlobalData.baseURL = BaseURL;
	}
	public void target(Element e) {
		String target = e.getText();
		GlobalData.target = target;
	}
	public void WebSite(Element e) {
		String WebSite = e.getText();
		GlobalData.WebSite = WebSite;
	}

	public void category(Element e) {
		String category = e.getText();
		GlobalData.category = category;
	}
	public void folder(Element e) {
		String folder = e.getText();
		GlobalData.folder = folder;
	}
	public String[] filter(Element e, String source, String parsertarget) {
		String[] datas = null;
		NodeFilter filter = null;
		String target = e.attributeValue("target");
		int position = -1;
		if (target.equals("child")) {
			position = Integer.parseInt(e.attributeValue("position"));
		}
		String tag = ((Element) e.selectNodes("tag").get(0)).getText();
		String key = ((Element) e.selectNodes("attribute/key").get(0))
				.getText();
		String value = ((Element) e.selectNodes("attribute/value").get(0))
				.getText();
		if(key.equals("") || value.equals(""))
			filter = new TagNameFilter(tag);
		else filter = new AndFilter(new TagNameFilter(tag), new HasAttributeFilter(
				key, value));
		try {
			Parser parser = new Parser(source);
			NodeList movielist = parser.parse(filter);
			if (movielist.size() > 0)
				datas = new String[movielist.size()];
			for (int i = 0; i < movielist.size(); i++) {
				Node movie = movielist.elementAt(i);
				String extract_href = "";
				if (target != null && target.equals("child"))
					extract_href = movie.getChildren().elementAt(position)
							.getText();
				else if (target != null && target.equals("self"))
					extract_href = movie.getText();
				if(GlobalData.WebSite.equals("youku")) {
					if(GlobalData.category.equals("star"))
						datas[i] = getDataForYoukuStar(extract_href,parsertarget);
					else
						datas[i] = getData(extract_href,parsertarget);
				}
			}
		} catch (ParserException e1) {
			ExceptionHandler.log("filter paser exception", e1);
		}
		return datas;
	}
	public String getData(String extract_href,String parsertarget) {
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
	public String getDataForYoukuStar(String extract_href,String parsertarget) {
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
	public String getDataShiguangwang(String extract_href,String parsertarget) {
		String href = "";
		int indexofhref = extract_href.indexOf(parsertarget);
		if(indexofhref < 0)
			return href;
		href = extract_href.substring(indexofhref);
		indexofhref = href.indexOf("\"") + 1;
		href = href.substring(indexofhref,
				href.indexOf("\"", indexofhref - 1));
		return href;
	}
	public void VisitandParse(Element e, String source) {
		if (source == null || source.equals(""))
			return;
		String target = e.attributeValue("target");
		String urltype = e.attributeValue("urltype");
		Element VisitandSave = null;
		if (urltype.equals("saveURL")) {
			//VisitandSave = (Element) e.selectNodes("VisitandSave").get(0);
			String[] datas = filter(((Element) e.selectNodes("filter").get(0)),
					source, target);
			if (datas != null && datas.length > 0) {
				for (int i = 0; i < datas.length; i++) {
					if(datas[i] != null && !datas[i].equals("")) {
						GlobalData.saveurls.add(datas[i]);	
					}
				}
			}
		}
		return;
	}

	public void VisitandSave(Element e, String href) {
		if (href == null || href.equals(""))
			return;
			String urlcontent = ConnectioinFuction.readURL(href);
			if(urlcontent == null || urlcontent.equals(""))
				return;
			String savefilename = e.attributeValue("savefilename");
			String filename = "";
			String str = "";
			if(savefilename != null && savefilename.equals("filter")) {
				String target = e.attributeValue("target");
				String[] datas = filter(((Element) e.selectNodes("filter").get(0)),
						urlcontent, target);
				if(datas != null&& datas.length > 0)
					str = datas[0];
			} else if(savefilename != null && savefilename.equals("Cuthref")) {
					str = href;
			}
			if(!str.equals("")) {
				String begin = e.attributeValue("begin");
				int countofbegin = Integer.parseInt(e.attributeValue("countofbegin"));
				int beginoffset = Integer.parseInt(e.attributeValue("beginoffset"));
				String end = e.attributeValue("end");
				int countofend = Integer.parseInt(e.attributeValue("countofend"));
				int endoffset = Integer.parseInt(e.attributeValue("endoffset"));
				int beginIndex = str.indexOf(begin);
				int endIndex = str.indexOf(end);
				for(int i = 0;i < countofbegin;i++) {
					beginIndex = str.indexOf(begin, beginIndex+1);
				}
				for(int i = 0;i < countofend;i++) {
					endIndex = str.indexOf(end, endIndex+1);
				}
				filename = str.substring(beginIndex+beginoffset, endIndex+endoffset);
				if(href.equals("http://www.youku.com/v")) 
					filename = "v";
			} else {
				filename =System.currentTimeMillis() + "";
			}
			String content = href + "\n" + urlcontent;
			String path = GlobalData.folder;
			if(path.equals("youku/columnhomepage")) {
				if(GlobalData.homepageCrawlerTime == 0)
					GlobalData.homepageCrawlerTime = System.currentTimeMillis();
					path += GlobalData.homepageCrawlerTime;
			}
			String filepath = path + "/" + filename + ".txt";
			////////////////
			File file = new File(filepath); 
			File parent = file.getParentFile(); 
			if(parent!=null&&!parent.exists()){ 
			parent.mkdirs(); 
			} 
			try {
				file.createNewFile();
			} catch (IOException e2) {
				e2.printStackTrace();
			} 
			 FileWriter fw;
			try {
				fw = new FileWriter(file);
				fw.write(content);
                fw.close();   
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			/////////////////////
	}

	public void Readfile(Element e) {
		Element visitandsave = null;
		Element visitandparse = null;
		String attribute = e.attributeValue("attribute");
		String urltype = e.attributeValue("urltype");
		String filePath = ((Element) e.selectNodes("filePath").get(0))
				.getText();
		String handlePattern = ((Element) e.selectNodes("handlePattern").get(0))
				.getText();
		HashSet<String> urls = new HashSet<String>();
		if(attribute != null && attribute.equals("urlParams")) {
			GlobalData.urlParams = urlParams(filePath);
			URLmaker.makeURL(0, null,urls);
		} else if(attribute != null && attribute.equals("saveURLs")) {
				urls = null;
				urls = saveURLs(filePath);
				if(GlobalData.folder.equals("youku/movies/zhishu/seriesAnalysis/viewAction")) {
					Iterator<String> iter = urls.iterator();
					HashSet<String> urlsFinal = new HashSet<String>();
					while(iter.hasNext()) {
						String url = iter.next();
						url = GlobalData.baseURL + url.substring(url.indexOf("id_"));
						urlsFinal.add(url);
					}
					urls = urlsFinal;
				}
		}
		if(urltype != null && urltype.equals("saveURL")) {
			GlobalData.saveurls = urls;
		}
		else if(urltype != null && urltype.equals("middleURL")) {
			GlobalData.middleurls = urls;
		}
		if(GlobalData.target != null && GlobalData.target.equals("saveURLs")) {
			Iterator<String> iterator = urls.iterator();
			List<?> VisitandParselist = e.selectNodes("VisitandParse");
			if (VisitandParselist.size() != 0) {
				visitandparse = (Element) VisitandParselist.get(0);
				while (iterator.hasNext()) {
					String URL = iterator.next();	
					String content = null;
					if (URL != null && !URL.equals(""))
					{
						while(true) {
							content = ConnectioinFuction.readURL(URL);
							if(content != null && !content.equals(""))
								break;
						}
					}
					if(GlobalData.category.equals("yugaopian")) {
						if(content.indexOf("抱歉，没有筛选到相关视频") > 0)
							continue;
					}
						VisitandParse(visitandparse,
								content);
				}
			}
			Iterator<String> saveURLiterator = GlobalData.saveurls.iterator();
			while(saveURLiterator.hasNext()) {
				FileHandler.writeSaveUrl(saveURLiterator.next());
			}
		}
		else if(GlobalData.target != null && GlobalData.target.equals("saveResource")) {
			Iterator<String> iterator = urls.iterator();
			List<?> VisitandSavelist = e.selectNodes("VisitandSave");
			if (VisitandSavelist.size() != 0) {
				visitandsave = (Element) VisitandSavelist.get(0);
				while (iterator.hasNext()) {
					String URL = iterator.next();
					if (URL != null && !URL.equals(""))
						VisitandSave(visitandsave,URL);
				}
			}
		}

	}

	public static String[][] urlParams(String filePath) {
		excecuteXML xml = new excecuteXML(filePath);
		Element e = xml.doc.getRootElement();
		int paramNum = Integer.parseInt(((Element) e.selectNodes("paramNum")
				.get(0)).getText());
		int paramMaxCount = Integer.parseInt(((Element) e.selectNodes(
				"paramMaxCount").get(0)).getText());
		GlobalData.paramNum = paramNum;
		GlobalData.paramMaxCount = paramMaxCount;
		String[][] urlParams = new String[paramNum][paramMaxCount];
		List<?> list = e.elements();
		for (int i = 0; i < list.size(); i++) {
			Element params = (Element) list.get(i);
			if (!params.getName().equals("params"))
				continue;
			int position = Integer.parseInt(params.attributeValue("position"));
			String changepattern = ((Element) params.selectNodes(
					"changepattern").get(0)).getText();
			if (changepattern.equals("read")) {
				List<?> paramlist = params.selectNodes("param");
				for (int j = 0; j < paramlist.size(); j++) {
					Element param = (Element) paramlist.get(j);
					String value = ((Element) param.selectNodes("value").get(0))
							.getText();
					String mean = ((Element) param.selectNodes("mean").get(0))
							.getText();
					urlParams[position][j] = value;
				}
			} else if (changepattern.equals("auto-increment")) {
				int minValue = Integer.parseInt(((Element) params.selectNodes(
						"minValue").get(0)).getText());
				int maxValue = Integer.parseInt(((Element) params.selectNodes(
						"maxValue").get(0)).getText());
				int index = 0;
				for (int j = minValue; j <= maxValue; j++) {
					urlParams[position][index] = j + "";
					index++;
				}
			}

		}
		return urlParams;
	}
	public HashSet<String> saveURLs(String filePath) {
		HashSet<String> urls = new HashSet<String>();
		try {
			BufferedReader bw = new BufferedReader(new FileReader(filePath));
			String url = null;
			while ((url = bw.readLine()) != null) {
				if (url != null && !url.equals(""))
					// GlobalData.saveurls.add(url);
					urls.add(url);
			}
			bw.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return urls;
	}
	
}
