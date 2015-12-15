package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseConnection {
	
		  //private   HTableInterface table;
		  private  static Configuration conf;
		  private  final  String url="192.168.10.10";
		  static{
		    conf =HBaseConfiguration.create();
		    conf.set("mapred.job.tracker", "192.168.10.10:9001");
		    conf.set("fs.default.name", "hdfs://192.168.10.10:9000");
		    conf.set("hbase.zookeeper.property.clientPort", "2181");
		    conf.set("hbase.zookeeper.quorum", "192.168.10.11");
		    conf.addResource(new Path("src/hbase-site.xml"));
		  }
		  private  static HTablePool hTablePool = null;
		  public  static Configuration getConf(){
		    return conf;
		  }
		  
		  public static HTablePool getHTablePool(){
			  return hTablePool;
		  }
		public   HTableInterface getHTable(String tablename){
			if(hTablePool == null) 
				hTablePool =new HTablePool(conf,1000);
			HTableInterface table=null;
			table= hTablePool.getTable(tablename); 
		    return table;
		  }
		  
		  public   byte[] gB(String name){
		    return Bytes.toBytes(name);
		  }
		
}
