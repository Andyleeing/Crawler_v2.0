package jxHan.Crawler.Util.XML;

import java.io.File;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class excecuteXML {
	Document doc;
	public excecuteXML(String filepath) {
		// TODO Auto-generated constructor stub
		SAXReader reader = new SAXReader();
		try {
			doc = reader.read(new File(filepath));

			} catch (DocumentException e) {
				//ExceptionHandler.log("SAXReader", e);
				e.printStackTrace();
		}
	}
	
	public void excecute() {
		if(doc == null) return;
		Element e = doc.getRootElement();
		List<?> list = e.elements();
		for(int i = 0;i < list.size();i++) {
			Element element = (Element)list.get(i);
			String name = element.getName();
			if(name.equals("WebSite")) XMLfunction.getXMLfunction().WebSite(element);
			else if(name.equals("category")) XMLfunction.getXMLfunction().category(element);
			else if(name.equals("target")) XMLfunction.getXMLfunction().target(element);
			else if(name.equals("BaseURL")) XMLfunction.getXMLfunction().BaseURL(element);
			else if(name.equals("folder")) XMLfunction.getXMLfunction().folder(element);
			else if(name.equals("ReadFile")) XMLfunction.getXMLfunction().Readfile(element);
		}
		}
}
