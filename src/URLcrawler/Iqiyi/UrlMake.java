package URLcrawler.Iqiyi;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class UrlMake {

	public HashMap MvurlMap = new HashMap();

	public void Mvurlconstructor() throws UnsupportedEncodingException {
		String[] countrnum = { "1", "2", "3", "4", "308", "1115", "5" };
		String[] year = { "2015", "2011_2014", "2000_2010", "1990_1999",
				"1980_1989", "1964_1979" };
		String temp_countr = null, temp_year = null;
		int index = 0;
		int page = 1;
		String baseurl = "http://list.iqiyi.com/www/1/";
		for (int i = 0; i < countrnum.length; i++) {
			temp_countr = countrnum[i];
			for (int j = 0; j < year.length; j++) {
				temp_year = year[j];

				String interurl = temp_countr + "-----------" + temp_year
						+ "--4-" + page + "-1-iqiyi--.html";
				MvurlMap.put(index, interurl);
				index++;
			}
		}
	}

	HashMap MvhurlMap = new HashMap();

	public void MvHurlconstructor() throws UnsupportedEncodingException {
		String[] countrnum = { "1013", "1012", "1011", "1010", "1009", "1008","1014","1015","1016" ,"1017","1018"};
		String[] count ={""};
		String temp_countr = null;
       String temp_type=null;
       String interurl=null;
		int index = 0;
		String[] type ={"1006","1007"};
		for(int j=0;j<type.length;j++) {
			temp_type=type[j];
			if(j==0) {
				for (int i = 6; i < countrnum.length; i++) {
					temp_countr = countrnum[i];
			 interurl =temp_type+"--"+ temp_countr + "-----------" + "4-1-2--1-.html";
			  MvhurlMap.put(index, interurl);
				index++;
				}
			}
			else if(j==1) {
				
				for (int i = 0; i < 6;i++) {
					temp_countr = countrnum[i];
					interurl =temp_type+"-"+ temp_countr + "------------" + "4-1-2--1-.html";
			  MvhurlMap.put(index, interurl);
				index++;
				}
			}
	
		}
	}
	
	private HashMap TvInurlMap = new HashMap();

	public void TvInurlconstructor() throws UnsupportedEncodingException {
		String[] countrnum = { "15", "16", "17", "18", "19", "309", "1114",
				"1117" };
		String[] year = { "2015", "2011_2014", "2000_2010", "1990_1999",
				"1980_1989", "1964_1979" };
		String temp_countr = null, temp_year = null;
		int index = 0;
		int page = 1;
		String baseurl = "http://list.iqiyi.com/www/2/";
		for (int i = 0; i < countrnum.length; i++) {
			temp_countr = countrnum[i];
			for (int j = 0; j < year.length; j++) {
				temp_year = year[j];

				String interurl = temp_countr + "-----------" + temp_year
						+ "--10-" + page + "-1-iqiyi--.html";
				TvInurlMap.put(index, interurl);
				index++;
			}
		}
	}

	private HashMap DmInurlMap = new HashMap();

	public void DmInurlconstructor() throws UnsupportedEncodingException {
		String[] countrnum = { "37", "38", "39", "40", "1105", "1106", "1107",
				"1145", "1194" };
		String temp_countr = null;
		int index = 0;
		int page = 1;
		String baseurl = "http://list.iqiyi.com/www/4/";
		for (int i = 0; i < countrnum.length; i++) {
			temp_countr = countrnum[i];
			String interurl = temp_countr + "--------------" + page
					+ "-1-iqiyi--.html";
			DmInurlMap.put(index, interurl);
			index++;
		}
	}

	private HashMap ZyInurlMap = new HashMap();

	public void ZyInurlconstructor() throws UnsupportedEncodingException {
		String[] typenum = { "100001", "155", "156", "157", "158", "159",
				"160", "161", "163", "292", "293", "1002", "1003", "2117",
				"2118", "2119", "2120", "2121", "2122", "2224" };

		String temp_type = null;
		int index = 0;
		int page = 1;
		String baseurl = "http://list.iqiyi.com/www/6/-";
		for (int i = 0; i < typenum.length; i++) {
			temp_type = typenum[i];
			String interurl = temp_type + "------------" + "10-" + page
					+ "-1-iqiyi--.html";
			ZyInurlMap.put(index, interurl);
			index++;
		}
	}

	public HashMap getDmInurlMap() {
		return DmInurlMap;
	}

	public void setDmInurlMap(HashMap urlMap) {
		this.DmInurlMap = urlMap;
	}

	public HashMap getTvInurlMap() {
		return TvInurlMap;
	}

	public void setTvInurlMap(HashMap urlMap) {
		this.TvInurlMap = urlMap;
	}

	public HashMap getMvurlMap() {
		return MvurlMap;
	}

	public void setMvurlMap(HashMap urlMap) {
		this.MvurlMap = urlMap;
	}

	public HashMap getMvhurlMap() {
		return MvhurlMap;
	}

	public void setMvhurlMap(HashMap urlMap) {
		this.MvhurlMap = urlMap;
	}

	public void setZyInurlMap(HashMap urlMap) {
		this.ZyInurlMap = urlMap;
	}

	public HashMap getZyInurlMap() {
		return ZyInurlMap;
	}
}
