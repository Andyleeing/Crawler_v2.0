package URLcrawler.Leshi;


import java.util.ArrayList;


public class leshiUrlCrawler {
	
	public static void LeshiUrl() {// 从目录页获取电视、动漫剧集URL
		String[] urlbank = {
				"http://list.letv.com/listn/c2_t-1_a-1_y-1_s1_md_o51_d1_p.html",// 电视剧
				"http://list.letv.com/listn/c5_t-1_a-1_y-1_vt-1_f-1_s1_lg-1_st-1_md_o9_d1_p.html",// 动漫
				"http://list.letv.com/listn/c11_t-1_a-1_s3_tv-1_md_o9_d1_p.html"// 综艺-栏目
		};
		
		ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i <= 2; i++) {
			list.addAll(LeshiFunction.getLeshiFun().urlmaker(urlbank[i]));//
			// 电视剧动漫综艺读出来的都是info链接
		}
		LeshiFunction.LeshiPlayCrawler(list, 1);// info链接放入多线程去寻找每一集的链接
		LeshiFunction.getLeshiFun().movieurl();// 从电影目录页读取电影以及片花的url

		
	}
}
