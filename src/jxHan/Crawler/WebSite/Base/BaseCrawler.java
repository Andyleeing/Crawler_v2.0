package jxHan.Crawler.WebSite.Base;
import jxHan.Crawler.Util.FileHandler;
import jxHan.Crawler.Util.Log.ExceptionHandler;
import jxHan.Crawler.Util.XML.excecuteXML;

public class BaseCrawler {
	excecuteXML xml;
	public BaseCrawler(String filepath) {
		// TODO Auto-generated constructor stub
		xml = new excecuteXML(filepath);
		
	}

	public void beginCrawl() {
		if(xml!= null)
			xml.excecute();
	}
	public void endCrawl() {
		FileHandler.endWrite();
		ExceptionHandler.endlog();
	}
}
