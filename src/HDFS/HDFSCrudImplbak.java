package HDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSCrudImplbak implements IHDFSCrud {

	Configuration conf=new Configuration();
	public static int count = 0;
	String uri="hdfs://192.168.0.210:9000";
	public static FileSystem hdfs;
	@Override
	public boolean create(String content,String filePath)  {
		//conf.set("fs.default.name", "hdfs://192.168.0.210:9000");
		try {
			//FileSystem hdfs=FileSystem.get(URI.create(uri),conf);
			//System.out.println(content);
			byte[] buff=content.getBytes();
			Path dst=new Path(filePath);
			//if(hdfs == null) {
			//	conf.set("fs.default.name", "hdfs://192.168.0.210:9000");
			//	hdfs=FileSystem.get(URI.create(uri),conf);
			//}
			FSDataOutputStream outputStream=hdfs.create(dst);
			outputStream.write(buff,0,buff.length);
			outputStream.flush();
			outputStream.close();
			//outputStream = null;
			//hdfs.close();
			buff = null;
			content = null;
			System.out.println(count++);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	
	@Override
	public boolean readHDFSListAll(String name, String path) {
		conf.set("fs.default.name", "hdfs://192.168.0.210:9000");
		FSDataInputStream inputStream=null;
		try {
			FileSystem hdfs=FileSystem.get(URI.create(uri), conf);
			BufferedReader buff=null;
			Path dst=new Path(path);
			FileStatus status[]=hdfs.listStatus(dst);
			int j=0;
			for(int i=0;i<status.length;i++){
				FileStatus temp[]=hdfs.listStatus(new Path(status[i].getPath().toString()));
				for(int k=0;k<temp.length;i++){
					System.out.println("file path = "+temp[k].getPath().toString());
					Path tempP=new Path(temp[k].getPath().toString());
					inputStream=hdfs.open(tempP);
					buff=new BufferedReader(new InputStreamReader(inputStream));
					String str=null;
					while((str=buff.readLine())!=null){
						System.out.print(str);
						
					}
					buff.close();
					inputStream.close();
					
				}
				hdfs.close();
				
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean update(String name, String path) {
		return false;
	}

	@Override
	public boolean delFile( String path) {
		conf.set("fs.default.name", "hdfs://192.168.0.210:9000");
		try {
			FileSystem hdfs=FileSystem.get(URI.create(uri), conf);
			Path dst=new Path(path);
			hdfs.deleteOnExit(dst);
			hdfs.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean delDir(String path) {
		conf.set("fs.default.name","hdfs://192.168.0.210:9000");
		try {
			FileSystem hdfs=FileSystem.get(URI.create(uri), conf);
			Path dst=new Path(path);
			hdfs.deleteOnExit(dst);
			hdfs.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return false;
	}

	@Override
	public boolean mkDir(String path) {
		conf.set("fs.default.name", "hdfs://192.168.0.210:9000");
		try {
			//FileSystem hdfs=FileSystem.get(URI.create(uri),conf);
			hdfs=FileSystem.get(URI.create(uri),conf);
			Path dst=new Path(path);
			System.out.println(dst);
			//FileStatus files[]=hdfs.listStatus(dst);
			//如何判重？
			hdfs.mkdirs(dst);
			System.out.println("make direction success - - - - - - ");
			//hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}




}
