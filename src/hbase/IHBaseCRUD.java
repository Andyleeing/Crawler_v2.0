package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ResultScanner;

public interface IHBaseCRUD {
	
	public void putRows(String tableName,String[] rows,String[] colfams,String[] quals,String[] values) throws Exception;
	public void createHTable(String tableName,String[] columns) throws IOException ;
	public void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException ;
	public void deleteRow(String tableName,String rowKey) throws IOException ;
	public ResultScanner queryAll(String tableName) throws IOException ;
	public ResultScanner queryByFQ(String tableName,String family,String qualifier) throws IOException ;
	


}
