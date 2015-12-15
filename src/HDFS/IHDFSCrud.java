package HDFS;

public interface IHDFSCrud {

	public boolean create(String content,String filePath );
	
	public boolean readHDFSListAll(String name,String path);
	
	public boolean update(String name,String path);
	
	public boolean delFile(String path);
	
	public boolean delDir(String path);
	
	public boolean mkDir(String path);
	
	
}
