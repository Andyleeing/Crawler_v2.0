package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCRUD implements IHBaseCRUD {

	//判断表中是否有该rowkry行的数据
	
		public static boolean exists(String tableName,String rowkey){
			Get get=new Get(Bytes.toBytes(rowkey));
			HTable table = null;
			try {
				table = new HTable(conf,Bytes.toBytes(tableName));
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
			Result rs = null;
			try {
				rs = table.get(get);
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
			byte[] key=rs.getRow();
			if(key==null) return false;
			else return true;
		}
	private static Configuration conf = HBaseConnection.getConf();
	private HBaseConnection hBaseConn = new HBaseConnection();
	public static Integer i = 0;
	// 插入到tablename里面

	public ArrayList<String> tableNames = new ArrayList<String>();
	public ArrayList<List<Put>> puts = new ArrayList<List<Put>>();

	public void commitPuts() throws Exception {
			if (tableNames.size() == 0)
				return;
			for (int i = 0; i < tableNames.size(); i++) {
				String tableName = tableNames.get(i);
				HTableInterface table = hBaseConn.getHTable(tableName);
				table.setAutoFlush(false);
				table.setWriteBufferSize(5*1024*1024);
				List<Put> list = puts.get(i);
				table.put(list);
				table.flushCommits();
				table.close();
				list.clear();
				System.out.println("success put into hbase :" + tableName);
			}
	}

	@Override
	public void putRows(String tableName, String[] rows, String[] colfams,
			String[] quals, String[] values) throws Exception {
			int index = -1;
			for (int i = 0; i < tableNames.size(); i++) {
				if (tableNames.get(i).equals(tableName)) {
					index = i;
					break;
				}
			}
			if (index >= 0) {
				List<Put> list = puts.get(index);
				for (int i = 0; i < rows.length; i++) {
					Put put = new Put(Bytes.toBytes(rows[i]));
					if (values[i] == null)
						values[i] = "";
					put.add(Bytes.toBytes(colfams[i]), Bytes.toBytes(quals[i]),
							Bytes.toBytes(values[i]));
					list.add(put);
				}
				if (list.size() >= 1000) {
					HTableInterface table = hBaseConn.getHTable(tableName);
					table.setAutoFlush(false);
					table.setWriteBufferSize(5*1024*1024);
					table.put(list);
					table.flushCommits();
					table.close();
					list.clear();
					System.out.println("success put into hbase :" + tableName);
				}
			} else {
				tableNames.add(tableName);
				List<Put> list = new ArrayList<Put>();
				puts.add(list);
				for (int i = 0; i < rows.length; i++) {
					Put put = new Put(Bytes.toBytes(rows[i]));
					if (values[i] == null)
						values[i] = "";
					put.add(Bytes.toBytes(colfams[i]), Bytes.toBytes(quals[i]),
							Bytes.toBytes(values[i]));
					list.add(put);
				}
				if (list.size() >= 1000) {
					HTableInterface table = hBaseConn.getHTable(tableName);
					table.setAutoFlush(false);
					table.setWriteBufferSize(5*1024*1024);
					table.put(list);
					table.flushCommits();
					table.close();
					list.clear();
					System.out.println("success put into hbase :" + tableName);
				}
			}
			synchronized(i) {
			System.out.println("putRows end " + (i++));
			}
	}

	// 创建HTable,需要tablename和 columns
	@Override
	public void createHTable(String tableName, String[] columns)
			throws IOException {

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table exists, trying to recreate table......");
			admin.enableTable(tableName);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor htd = new HTableDescriptor(tableName);
		for (int i = 0; i < columns.length; i++) {
			HColumnDescriptor col = new HColumnDescriptor(columns[i]);
			htd.addFamily(col);
		}
		System.out.println("create new table:" + tableName);
		admin.createTable(htd);
	}

	// 删除tablename的HTable

	@Override
	public void dropTable(String tableName) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			if (!admin.isTableEnabled(tableName)) {
				admin.enableTable(tableName);
			}
			if (!admin.isTableDisabled(tableName))
				admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(tableName + "is exist,delete......");
		}
	}

	// 删除某一行数据
	@Override
	public void deleteRow(String tableName, String rowKey) throws IOException {
		HTableInterface table = hBaseConn.getHTable(tableName);
		List list = new ArrayList();
		Delete del = new Delete(rowKey.getBytes());// 可以类似于puts，删除多行
		list.add(del);

		table.delete(list);
		System.out.println("删除成功" + tableName + rowKey);
	}

	// 查询tableName所有的数据
	@Override
	public ResultScanner queryAll(String tableName) throws IOException {
		HTableInterface table = hBaseConn.getHTable(tableName);
		ResultScanner rs = table.getScanner(new Scan());
		return rs;
	}

	// 查询指定列族和列，如果不指定列，设为null
	@Override
	public ResultScanner queryByFQ(String tableName, String family,
			String qualifier) throws IOException {
		HTableInterface table = hBaseConn.getHTable(tableName);
		ResultScanner rs = null;
		if (qualifier != null)
			rs = table.getScanner(family.getBytes(), qualifier.getBytes());
		else
			rs = table.getScanner(family.getBytes());
		for (Result r : rs) {
			System.out.println("获得到rowkey:" + new String(r.getRow()));
			for (KeyValue keyValue : r.raw()) {
				System.out.println("列：" + new String(keyValue.getFamily())
						+ "====值:" + new String(keyValue.getValue()));
			}
		}
		return rs;
	}

	// 查询指定列族和列，设置其实行
	public ResultScanner queryByStartEnd(String tableName, String startRow,
			String endRow) throws IOException {
		HTableInterface table = hBaseConn.getHTable(tableName);
		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		if (endRow != null)
			scan.setStopRow(endRow.getBytes());
		ResultScanner rs = table.getScanner(scan);

		return rs;
	}
}
