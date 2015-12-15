package ParserData.YoukuParserData;

import hbase.HBaseCRUD;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import Utils.JDBCConnection;
import Utils.TextValue;


public class ImportThread1Video_v2 implements Runnable {
	public ArrayList<ImportThread1Video_v2> pool;
	public static Iterator<Result> ite1;
	public JDBCConnection jdbconn = new JDBCConnection();
	public static Map map1 = new HashMap();
	
	public static ArrayList reflist = new ArrayList();
	public static HBaseCRUD crud1 = new HBaseCRUD();
	public static int count = 0;
	@Override
	public void run() {
		while (true) {
			Result r = null;
				synchronized(ite1) {
					if(ite1.hasNext()) {
						r = ite1.next();
					} else {
						synchronized (pool) {
							jdbconn.closeConn();
							pool.remove(this);
							System.out
									.println("a thread removed from pool  pool size:"
											+ pool.size()
											+ ""
											+ new Date().toString());
							break;
						}
					}
			}
			try {
				importData(r);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void importData(Result r) throws Exception {
			youkuImport(r);
	}

	public static String  cleanString(String str){
		str=str.replaceAll(" ", "");
		str=str.replaceAll("'", "");
		str=str.replaceAll("-", "");
		str=str.replaceAll("\n", "");
		str=str.replaceAll("\r", "");
		str=str.replaceAll("//s", "");
		return str;
	}
	public void youkuImport(Result r1) throws Exception {
				byte[] key1 = r1.getRow();
				String keyString1 = new String(key1, "utf-8");
				
				int flagindex = keyString1.lastIndexOf("+");
				if(keyString1.indexOf("_di/egap_wohs/moc.ukuoy.www") < 0)
					return;
				int index1 = keyString1.lastIndexOf("yk");
				int index2 = keyString1.indexOf("+", index1);
				int index3 = keyString1.indexOf("+", index2 + 1);
				String flagString = "";
				if (index2 > 0 && index3 > 0 && index3 > index2)
					flagString = keyString1.substring(index2 + 1, index3);
				int index4 = keyString1.lastIndexOf("+");
				String timeString = "";
				String timeString1 = "";
				if (index4 > 0) {
					timeString = keyString1.substring(index4 + 1).substring(0,
							10);
					timeString1 = keyString1.substring(index4 + 1).substring(0,
							13);
				}
				int timeStamp = ConvertToInt(timeString);
				int timeStamp1 = ConvertToInt(timeString1);
				
				if (timeStamp < 1420041600) {
					crud1.deleteRow_videodynamicbak(keyString1);
					return;
				}
				
				if (flagString.equals("n")) {
					
					// //////////recommend/////////////////////
					if (timeStamp > 1422547200) {
						byte[] recommend = r1.getValue("C".getBytes(),
								"recommend".getBytes());
						if (recommend != null) {
							String recommendString = new String(recommend, "utf-8");
							if (!recommendString.equals("")) {
								// parse
								int timeint = Integer.parseInt(timeString);
								SimpleDateFormat sdf = new SimpleDateFormat(
										"yyyyMMdd");
								String sd = sdf.format(new Date(Long
										.parseLong(timeint + "000")));
								// FileWriter fw = new FileWriter("src/recommends/"
								// + sd, true);
								int indexcode = recommendString.indexOf("codeId");
								while (indexcode >= 0) {
									int indexcodeend = recommendString.indexOf(
											"\"", indexcode + 9);
									if (indexcodeend >= 0) {
										String codeid = recommendString.substring(
												indexcode + 9, indexcodeend);
										StringBuilder infourl = new StringBuilder();
										if (codeid.charAt(0) == 'X') {
											infourl.append("http://v.youku.com/v_show/id_");
											infourl.append(codeid);
											infourl = infourl.reverse();
											addreference(infourl.toString(), sd, 2);
										} else {
											infourl.append("http://www.youku.com/show_page/id_z");
											infourl.append(codeid);
											infourl = infourl.reverse();
											infourl.append("+yk");
											addreference(infourl.toString(), sd, 1);
										}

										indexcode = recommendString.indexOf(
												"codeId", indexcode + 10);
									} else
										break;
								}
							}
						}
					}
					// //////////recommend end////////////////////
					byte[] inforowkey = r1.getValue("R".getBytes(),
							"inforowkey".getBytes());
					if (inforowkey == null) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", "", 1, "yk", "", "no inforowkey", 2);
						
						return;
					}
					String inforowkeyString = new String(inforowkey);
					if(inforowkeyString.equals("")) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", "", 1, "yk", "", "no inforowkey", 2);
						
						return;
					}
					byte[] playrowkey = r1.getValue("R".getBytes(),
							"playrowkey".getBytes());
					if (playrowkey == null) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", "", 1, "yk", "", "no playrowkey", 2);
						
						return;
					}
					String playrowkeyString = new String(playrowkey);
					if(playrowkeyString.equals("")) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", "", 1, "yk", "", "no playrowkey", 2);
						
						return;
					}
					
					
					int up = -1;
					int down = -1;
					byte[] updown = r1.getValue("C".getBytes(),
							"updown".getBytes());
					if (updown != null) {
						String updownString = new String(updown, "utf-8");
						if (!(updownString.indexOf("观众过少") >= 0)) {
							try {
								int index = updownString.indexOf("@");
								if (index > 0) {
									up = ConvertToInt(updownString.substring(0,
											index));
									down = ConvertToInt(updownString
											.substring(index + 1));
								}
							} catch (Exception e) {

							}
						}
					}
					int sumplay = -1;
					byte[] sumplaycount = r1.getValue("C".getBytes(),
							"sumplaycount".getBytes());
					if (sumplaycount != null) {
						//crud1.deleteRow_videodynamicbak(keyString1);
						//return;
						String sumplaycountString = new String(sumplaycount,
								"utf-8");
						sumplay = ConvertToInt(sumplaycountString);
					} 
				    
					if(sumplay == -1){
						
						SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
					    String sd1 = sdf1.format(new Date(Long.parseLong(timeString1)));  
					    System.out.println(sd1 + "    " + sumplay);
					    
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", playrowkeyString, 1, "yk", "", "no sumplaycount", 2);
						return;
					}
					
					int commentCount = -1;
					byte[] comment = r1.getValue("C".getBytes(),
							"comment".getBytes());
					if (comment != null) {
						String commentString = new String(comment, "utf-8");
						commentCount = ConvertToInt(commentString);
					}
					ArrayList<TextValue> values = new ArrayList<TextValue>();
					TextValue rowkeytv = new TextValue();
					rowkeytv.text = "rowkey";
					rowkeytv.value = inforowkeyString + "+yk";
					values.add(rowkeytv);

					TextValue inforowkeytv = new TextValue();
					inforowkeytv.text = "inforowkey";
					inforowkeytv.value = inforowkeyString;
					values.add(inforowkeytv);

					TextValue playrowkeytv = new TextValue();
					playrowkeytv.text = "playrowkey";
					playrowkeytv.value = playrowkeyString;
					values.add(playrowkeytv);

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

					TextValue timestamptv = new TextValue();
					timestamptv.text = "timestamp";
					timestamptv.value = timeStamp;
					values.add(timestamptv);

					TextValue namewebsite = new TextValue();
					namewebsite.text = "website";
					namewebsite.value = "yk";
					values.add(namewebsite);

					TextValue collect = new TextValue();
					collect.text = "collect";
					collect.value = -1;
					values.add(collect);
					
					TextValue outside = new TextValue();
					outside.text = "outside";
					outside.value = -1;
					values.add(outside);
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					String sd = sdf.format(new Date(Long.parseLong(timeStamp
							+ "000")));
					jdbconn.insert(values, "videodynamic" + sd);
				} else if (flagString.equals("y")) {
					
					int index_1 = keyString1.indexOf("+");
					String inforowkeyString = keyString1.substring(0,index_1);
					int index_2 = keyString1.indexOf("+",index_1 + 1);
					String playrowkeyString = keyString1.substring(index_1 + 1,index_2);
					
					
					byte[] collect = r1.getValue("C".getBytes(),
							"collect".getBytes());
					if (collect == null) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", playrowkeyString, 1, "yk", "", "no collect data", 2);
						
						return;
					}
					String collectString = new String(collect, "utf-8");
					int collectint = ConvertToInt(collectString);
					if (collectint < 0) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", playrowkeyString, 1, "yk", "", "no collect data", 2);
						return;
					}

					byte[] outside = r1.getValue("C".getBytes(),
							"outside".getBytes());
					if (outside == null) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", playrowkeyString, 1, "yk", "", "no outside data", 2);
						
						return;
					}
					String outsideString = new String(outside, "utf-8");
					int outsideint = ConvertToInt(outsideString);
					if (outsideint < 0) {
						crud1.deleteRow_videodynamicbak(keyString1);
						jdbconn.log("hanjiangxue", playrowkeyString, 1, "yk", "", "no outside data", 2);
						
						return;
					}
					ArrayList<TextValue> values = new ArrayList<TextValue>();
					TextValue collecttv = new TextValue();
					collecttv.text = "collect";
					collecttv.value = collectint;
					values.add(collecttv);

					TextValue outsidetv = new TextValue();
					outsidetv.text = "outside";
					outsidetv.value = outsideint;
					values.add(outsidetv);
					
					
					TextValue rowkeytv = new TextValue();
					rowkeytv.text = "rowkey";
					rowkeytv.value = inforowkeyString + "+yk";
					values.add(rowkeytv);

					TextValue inforowkeytv = new TextValue();
					inforowkeytv.text = "inforowkey";
					inforowkeytv.value = inforowkeyString;
					values.add(inforowkeytv);

					TextValue playrowkeytv = new TextValue();
					playrowkeytv.text = "playrowkey";
					playrowkeytv.value = playrowkeyString;
					values.add(playrowkeytv);

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

					TextValue up = new TextValue();
					up.text = "up";
					up.value = -1;
					values.add(up);
					
					TextValue down = new TextValue();
					down.text = "down";
					down.value = -1;
					values.add(down);
					
					TextValue sumplaycount = new TextValue();
					sumplaycount.text = "sumplaycount";
					sumplaycount.value = -1;
					values.add(sumplaycount);
					
					TextValue comment = new TextValue();
					comment.text = "comment";
					comment.value = -1;
					values.add(comment);
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
					String sd = sdf.format(new Date(Long.parseLong(timeStamp
							+ "000")));
					if (jdbconn.insert(values, "videodynamic" + sd) == -1) {
						System.out.println("insert error ");
					}
				}
				crud1.deleteRow_videodynamicbak(keyString1);
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

	public static void addreference(String infourl, String sd, int flag) {
		// /flag 1:moviedynamic 2:videodynamic
		if(flag == 2)
			return;
		String table = "";
		if (flag == 1) {
			table = "moviedynamic" + sd;
		} else if (flag == 2) {
			table = "videodynamic" + sd;
		}
		String key = infourl + "@@@" + table;
		synchronized (reflist) {
			int done = 0;
			for(int i = 0;i < reflist.size();i++) {
				TextValue tv = (TextValue)reflist.get(i);
				if(tv.text.equals(key)) {
					tv.value = Integer.parseInt(tv.value.toString())+ 1;
					done = 1;
					break;
				}
			}
			if(done == 0) {
				TextValue tv = new TextValue();
				tv.text = key;
				tv.value = 1;
				reflist.add(tv);
			}
		}
	}
}
