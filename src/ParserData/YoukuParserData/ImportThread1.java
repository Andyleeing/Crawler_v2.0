package ParserData.YoukuParserData;


import hbase.HBaseCRUD;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import Utils.TextValue;

public class ImportThread1 implements Runnable {
	public static ArrayList<String> keyStringList;
	public static ArrayList<String> webList;
	public static ArrayList<String> categoryList;
	public ArrayList<ImportThread1> pool;
	public JDBCConnection jdbconn = new JDBCConnection();
	PeopleSplit people = new PeopleSplit();
	Province province = new Province();
	public static HBaseCRUD crud1 = new HBaseCRUD();

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			String keyString = "";
			String webString = "";
			String categoryString = "";
			synchronized (keyStringList) {
				if (keyStringList != null && keyStringList.size() > 0) {
					keyString = keyStringList.get(0);
					keyStringList.remove(0);
					webString = webList.get(0);
					webList.remove(0);
					categoryString = categoryList.get(0);
					categoryList.remove(0);
				} else if (keyStringList == null || keyStringList.size() == 0) {
					synchronized (pool) {
						jdbconn.closeConn();
						pool.remove(this);
						System.out.println("a thread removed from pool  pool size:" + pool.size() + ""
								+ new Date().toString());
						break;
					}
				}
			}
			try {
				importData(keyString, webString,categoryString);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void importData(String keyString, String webString,String categoryString) throws Exception {
		
		////////////这里要把category做为参数传递给各自的函数//////////////////////////
		///////////判断各自的category，写入mysql时统一：电影：movie   电视剧 tv  综艺  zongyi  动漫   dongman
		
		if (webString.equals("yk"))
			youkuImport(keyString,categoryString);
	}
	public void youkuImport(String keyString,String categoryString) throws Exception {

		ResultScanner rs1 = null;
		try {
			rs1 = crud1.queryByStartEnd("moviedynamicbak2", keyString, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Iterator<Result> ite1 = rs1.iterator();
		try {
			while (ite1.hasNext()) {
				Result r1 = ite1.next();
				byte[] key1 = r1.getRow();
				if (key1 == null) {
					break;
				}
				String keyString1 = new String(key1, "utf-8");
				int index1 = keyString1.indexOf("yk");
				if (index1 < 0)
					break;
				String subKeyString = keyString1.substring(0, index1 + 2);
				if (!subKeyString.equals(keyString))
					break;

				int index2 = keyString1.indexOf("+", index1);
				int index3 = keyString1.indexOf("+", index2 + 1);
				String flagString = "";
				if (index2 > 0 && index3 > 0 && index3 > index2)
					flagString = keyString1.substring(index2 + 1, index3);

				int index4 = keyString1.lastIndexOf("+");
				String timeString = "";
				if (index4 > 0)
					timeString = keyString1.substring(index4 + 1).substring(0,
							10);
				int timeStamp = ConvertToInt(timeString);
				if (timeStamp < 1420041600) {
					crud1.deleteRow_moviedynamicbak(keyString1);
					continue;
				}
				if (flagString.equals("n")) {
					
					int up = -1;
					int down = -1;
					byte[] updown = r1.getValue("C".getBytes(),
							"updown".getBytes());
					if (updown != null){
						String updownString = new String(updown, "utf-8");
						if (!(updownString.indexOf("观众过少") >= 0)) {
							int indexY = updownString.indexOf("有");
							int indexY2 = updownString.indexOf("有", indexY + 1);
							int indexR = updownString.indexOf("人");
							int indexR2 = updownString.indexOf("人", indexR + 1);
							if (indexR >= 0 && indexY >= 0 && indexR > indexY) {
								up = ConvertToInt(updownString.substring(
										indexY + 1, indexR));
							}
							if (indexR2 >= 0 && indexY2 >= 0 && indexR2 > indexY2) {
								down = ConvertToInt(updownString.substring(
										indexY2 + 1, indexR2));
							}
						}
					}
					int sumplay = -1;
					byte[] sumplaycount = r1.getValue("C".getBytes(),
							"sumplaycount".getBytes());
					if (sumplaycount != null){
						String sumplaycountString = new String(sumplaycount,
								"utf-8");
						sumplay = ConvertToInt(sumplaycountString);
					}
					if(sumplay == -1){
						crud1.deleteRow_moviedynamicbak(keyString1);
						jdbconn.log("hanjiangxue", keyString, 1, "yk", "", "no sumplaycount", 2);
						continue;
					}
					
					byte[] score = r1.getValue("C".getBytes(),
							"score".getBytes());
					double scoredouble = -1;
					if(score != null) {
						String scoreString =  new String(score,"utf-8");
						try {
							scoredouble = Double.parseDouble(scoreString);
						}catch(Exception e) {
						}
					}
					int commentCount = -1;
					byte[] comment = r1.getValue("C".getBytes(),
							"comment".getBytes());
					if (comment != null) {
						String commentString = new String(comment, "utf-8");
						commentCount = ConvertToInt(commentString);
					}
					
					int todayCount = -1;
					byte[] todayplaycount = r1.getValue("C".getBytes(),
							"todayplaycount".getBytes());
					if (todayplaycount != null){
						String todayplaycountString = new String(todayplaycount,
								"utf-8");
						todayCount = ConvertToInt(todayplaycountString);
					}
					
					String free = "1";
					byte[] freebyte = r1.getValue("C".getBytes(),
							"free".getBytes());
					if (freebyte != null){
						free = new String(freebyte,
								"utf-8");
					}
					
					ArrayList<TextValue> values = new ArrayList<TextValue>();
					
					TextValue freetv = new TextValue();
					freetv.text = "free";
					freetv.value = free;
					values.add(freetv);
					
					TextValue rowkeytv = new TextValue();
					rowkeytv.text = "rowkey";
					rowkeytv.value = keyString;
					values.add(rowkeytv);

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

					TextValue flagtv = new TextValue();
					flagtv.text = "flag";
					flagtv.value = 1;// n 2:y
					values.add(flagtv);

					TextValue commenttv = new TextValue();
					commenttv.text = "comment";
					commenttv.value = commentCount;// n 2:y
					values.add(commenttv);

					TextValue todayPlayCounttv = new TextValue();
					todayPlayCounttv.text = "todayPlayCount";
					todayPlayCounttv.value = todayCount;
					values.add(todayPlayCounttv);

					TextValue timestamptv = new TextValue();
					timestamptv.text = "timestamp";
					timestamptv.value = timeStamp;
					values.add(timestamptv);

					TextValue namewebsite = new TextValue();
					namewebsite.text = "website";
					namewebsite.value = "yk";
					values.add(namewebsite);
					
					TextValue categorytv = new TextValue();
					categorytv.text = "category";
					categorytv.value = categoryString;
					values.add(categorytv);
					
					TextValue reftv = new TextValue();
					reftv.text = "reference";
					reftv.value = 0;
					values.add(reftv);
					
					TextValue scoretv = new TextValue();
					scoretv.text = "score";
					scoretv.value = scoredouble;
					values.add(scoretv);

					SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
					String sd = sdf.format(new Date(Long.parseLong(timeStamp +"000")));
					jdbconn.insert(values, "moviedynamic"+sd);
				} else if (flagString.equals("y")) {
					byte[] youkupeople = r1.getValue("C".getBytes(),
							"youkupeople".getBytes());
					if (youkupeople == null){
						crud1.deleteRow_moviedynamicbak(keyString1);
						jdbconn.log("hanjiangxue", keyString, 1, "yk", "", "no youkupeople data", 2);
						continue;
					}
					String youkupeopleString = new String(youkupeople, "utf-8");

					ArrayList<TextValue> values = new ArrayList<TextValue>();

					TextValue rowkeytv = new TextValue();
					rowkeytv.text = "rowkey";
					rowkeytv.value = keyString;
					values.add(rowkeytv);

					TextValue categorytv = new TextValue();
					categorytv.text = "category";
					categorytv.value = categoryString;
					values.add(categorytv);
					
					TextValue flagtv = new TextValue();
					flagtv.text = "flag";
					flagtv.value = 2;// n 2:y
					values.add(flagtv);

					TextValue timestamptv = new TextValue();
					timestamptv.text = "timestamp";
					timestamptv.value = timeStamp;
					values.add(timestamptv);

					TextValue namewebsite = new TextValue();
					namewebsite.text = "website";
					namewebsite.value = "yk";
					values.add(namewebsite);
					
					TextValue sumcount = new TextValue();
					sumcount.text = "sumplaycount";
					sumcount.value = -1;
					values.add(sumcount);
					
					TextValue up = new TextValue();
					up.text = "up";
					up.value = -1;
					values.add(up);
					
					TextValue down = new TextValue();
					down.text = "down";
					down.value = -1;
					values.add(down);
					
					TextValue score = new TextValue();
					score.text = "score";
					score.value = -1;
					values.add(score);
					
					TextValue comment = new TextValue();
					comment.text = "comment";
					comment.value = -1;
					values.add(comment);
					
					TextValue todayplaycount = new TextValue();
					todayplaycount.text = "todayplaycount";
					todayplaycount.value = -1;
					values.add(todayplaycount);
	
					TextValue reference = new TextValue();
					reference.text = "reference";
					reference.value = -1;
					values.add(reference);
					///////////////////////////////////////////
					people.clear();
					people.getAge(youkupeopleString);
					for (int i = 0; i < 4; i++) {
						TextValue tv = new TextValue();
						tv.text = "age" + i;
						tv.value = people.age[i];
						values.add(tv);
					}
					for (int i = 0; i < 4; i++) {
						TextValue tv = new TextValue();
						tv.text = "occ" + i;
						tv.value = people.occ[i];
						values.add(tv);
					}
					TextValue tv = new TextValue();
					tv.text = "man";
					tv.value = people.sex[0];
					values.add(tv);

					TextValue tv1 = new TextValue();
					tv1.text = "women";
					tv1.value = people.sex[1];
					values.add(tv1);

					byte[] youkuarea = r1.getValue("C".getBytes(),
							"youkuarea".getBytes());
					if (youkuarea == null){
						crud1.deleteRow_moviedynamicbak(keyString1);
						jdbconn.log("hanjiangxue", keyString, 1, "yk", "", "no youkuarea data", 2);
						
						continue;
					}

					String youkuareaString = new String(youkuarea, "utf-8");
					province.clear();
					province.province(youkuareaString);
					for (int i = 0; i < 36; i++) {

						if (province.pro[i][0] < 0)
							continue;
						TextValue tvp = new TextValue();
						tvp.text = "area" + province.pro[i][0];
						tvp.value = province.pro[i][1];
						values.add(tvp);
					}

					SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
					String sd = sdf.format(new Date(Long.parseLong(timeStamp +"000")));  
					if (jdbconn.insert(values, "moviedynamic"+sd) == -1) {
						crud1.deleteRow_moviedynamicbak(keyString1);
						jdbconn.log("hanjiangxue", keyString, 1, "yk", "", "insert error", 2);
						continue;
					}
				}
				crud1.deleteRow_moviedynamicbak(keyString1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs1 != null)
				rs1.close();
		}
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
	public static double ConvertToDouble(String str){
		double value=0.0;
		str = str.replaceAll(",", "").replaceAll("\t", "");
		try {
			value = Double.parseDouble(str);
		} catch (Exception e) {
		}
		return value;
	}
}
