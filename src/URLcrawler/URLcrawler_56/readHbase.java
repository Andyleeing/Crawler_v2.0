package URLcrawler.URLcrawler_56;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import hbase.HBaseCRUD;

public class readHbase 
{
	static FileWriter fw=null;
	public static void main(String[] args) 
	{
		HBaseCRUD hbase=new HBaseCRUD();
		ResultScanner rs=null;
		try 
		{
			fw=new FileWriter("/root/YoukuCrawler_v1/src/syl/urlHbase.txt");
		} 
		catch (IOException e1) 
		{
			e1.printStackTrace();
		}
		try 
		{
			rs=hbase.queryAll("url56new");
			Iterator<Result> ite=rs.iterator();
			int count=0;
			while(ite.hasNext())
			{
				Result r=ite.next();
				String keyString="";
				String valueString="";
				byte[] key=r.getRow();
				if(key!=null) keyString=new String(key,"utf-8");
				byte[] value=r.getValue("C".getBytes(), "url".getBytes());
				if(value!=null) valueString=new String(value,"utf-8");
				count++;
//				System.out.println(count);
//				System.out.println(keyString);
//				System.out.println(valueString);
				fw.write(valueString+"\n");
				fw.flush();
				hbase.deleteRow("url56new",keyString);
				//System.out.println("delete "+keyString);
				
			}	
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}	
				
		
	}
	
}
