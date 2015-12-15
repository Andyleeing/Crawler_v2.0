package DataCrawler.YoukuDataCrawler;

import Utils.JDBCConnection;

public class YoukuDataCrawler {

	public dongmanFunction dongman;
	public MovieFuction movie;
	public tvFunction tv;
	public zongyiFunction zongyi;
	public long time;
	public String date;
	public JDBCConnection jdbc;
	public YoukuDataCrawler(long time, String date,JDBCConnection jdbc) {
		this.time = time;
		this.date = date;
		this.jdbc = jdbc;
		dongman = new dongmanFunction(jdbc);
		movie = new MovieFuction(jdbc);
		tv = new tvFunction(jdbc);
		zongyi = new zongyiFunction(jdbc);
		
	}
	public int crawler(String url) {
		int flag = 1;
		String splits[] = url.split(" ");
		if(splits.length > 0 ) {
			if(splits[0].equals("youku")) {
				if(splits.length > 1) {
					if(splits[1].equals("dongman")) {
						if(splits.length > 2) {
							if(splits[2].equals("info")) {
								if(splits.length > 3)
									flag = dongman.infoCrawler(splits[3], time, date);
							} else if(splits[2].equals("play")) {
								if(splits.length > 3)
									flag = dongman.playCrawler(url.substring(url.indexOf(splits[2]) + 5), time, date);
							}
						}
					} else if(splits[1].equals("movie")) {
						if(splits.length > 2) {
							if(splits[2].equals("info")) {
								if(splits.length > 3)
									flag = movie.infoCrawler(splits[3], time, date);
							} else if(splits[2].equals("play")) {
								if(splits.length > 3) 
									flag = movie.playCrawler(url.substring(url.indexOf(splits[2]) + 5), time, date);
							}
						}
					} else if(splits[1].equals("tv")) {
						if(splits.length > 2) {
							if(splits[2].equals("info")) {
								if(splits.length > 3)
									flag = tv.infoCrawler(splits[3], time, date);
							} else if(splits[2].equals("play")) {
								if(splits.length > 3) 
									flag = tv.playCrawler(url.substring(url.indexOf(splits[2]) + 5), time, date);
							}
						}
					} else if(splits[1].equals("zongyi")) {
						if(splits.length > 2) {
							if(splits[2].equals("info")) {
								if(splits.length > 3)
									flag = zongyi.infoCrawler(splits[3], time, date);
							} else if(splits[2].equals("play")) {
								if(splits.length > 3) 
									flag = zongyi.playCrawler(url.substring(url.indexOf(splits[2]) + 5), time, date);
							} 
						}
					}
				}
			}
		}
		return flag;
	}
}
