package Utils;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class JDBCConnection {

	private Connection conn = null;
	private static String user = "root";
	private static String password = "123";
	private static String url = "jdbc:mysql://192.168.0.117:3306/moviedata";
	private static String driver = "com.mysql.jdbc.Driver";
	public Connection getConn() {
		if(conn == null) {
			try{
				Class.forName(driver);
				conn = DriverManager.getConnection(url, user, password);
				//conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/moviedata?useUnicode=true&characterEncoding=utf-8" );
	         }catch(Exception e) {
	        	 e.printStackTrace();
	         }
		}
		return conn;
	}
	public void closeConn() {
		if(conn != null) {
			try {
			conn.close();
			conn = null;
			}catch(Exception e) {
			}
		}
	}
	public ResultSet executeQuerySingle(String sql) {
	        ResultSet rs = null;
			try {
				rs = getConn().createStatement().executeQuery(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
	        return rs;
    }
	//added by hanjiaxing 
		public int executeQueryCount(String sql) {
			PreparedStatement pstm = null;
			ResultSet rs = null;
			int count = 0;

			try {
				pstm = getConn().prepareStatement(sql);
				rs = pstm.executeQuery();
				rs.next();
				count = rs.getInt("count");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return count;
		}
		
	public ResultSet tableExist(String sql) {
		ResultSet rs = null;
		try {
			rs = getConn().createStatement().executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if(!rs.next())
				return null;
		} catch (SQLException e) {
		}
		return rs;
	}
	
	
	public void updateBatch(ArrayList sqls) {
		try {
			Connection conn = getConn();
			conn.setAutoCommit(false);
			for(int i = 0;i < sqls.size();i++) {
				Statement statement = conn.createStatement();
				statement.executeUpdate (sqls.get(i).toString());
			}
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public int update(String sql) {
		System.out.println(sql);
		int flag = 1;
		Statement statement = null;
		try {
			statement = getConn().createStatement();
			flag = statement.executeUpdate (sql);
		} catch (SQLException e) {
			flag = -1;
			e.printStackTrace();
		}
		return flag;
	}
	
	public int insert(ArrayList<TextValue> values,String table) {
		int rs = -1;
		Statement statement = null;
		String sql = "";
		try {
			statement = getConn().createStatement();
			sql = "insert into " + table;
			String sqlset = " (";
			String sqlvalues = "values(";
			int count = values.size();
			for(int i = 0;i < count;i++) {
				sqlset += values.get(i).text;
				sqlvalues += "'" + values.get(i).value + "'";
				if(i < count - 1) {
					sqlset += ",";
					sqlvalues += ",";
				}
			}
			sqlset += ") ";
			sqlvalues += ");";
			sql = sql + sqlset + sqlvalues;
			rs = statement.executeUpdate (sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			System.out.println(sql);
		}
		return rs;
	}
	
	public int insert_newtable(ArrayList<TextValue> values,String table) {
		int rs = -1;
		Statement statement = null;
		String sql = "";
		try {
			statement = getConn().createStatement();
			sql = "insert into " + table;
			String sqlset = " (";
			String sqlvalues = "values(";
			int count = values.size();
			for(int i = 0;i < count;i++) {
				sqlset += values.get(i).text;
				sqlvalues += "'" + values.get(i).value + "'";
				if(i < count - 1) {
					sqlset += ",";
					sqlvalues += ",";
				}
			}
			sqlset += ") ";
			sqlvalues += ");";
			sql = sql + sqlset + sqlvalues;
			rs = statement.executeUpdate (sql);
		} catch (SQLException e) {
			System.out.println(sql);
			e.printStackTrace();
		}
		return rs;
	}
	
	  public int log(String manager, String rowkey, int level, String website,
              String url, String content, int type) {
      //type 1:crawl 2:parse
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      String date = sdf.format(new Date());
      String machine = SysParams.urlTable_Hbase_local;
      String sql = "insert into Log"
                      + date
                      + " (machine,level,rowkey,url,time,content,website,manager,type) values ('"
                      + machine + "','" + level + "','" + rowkey + "','" + url
                      + "',now(),'" + content + "','" + website + "','" + manager
                      + "','" + type + "')";
      //System.out.println(sql);
      int flag = 1;
      Statement statement = null;
      try {
              statement = getConn().createStatement();
              flag = statement.executeUpdate(sql);
      } catch (SQLException e) {
              flag = -1;
              e.printStackTrace();
      }
      return flag;
}

	
}
