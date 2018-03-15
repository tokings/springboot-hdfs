package com.embraces.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hive JDBC 带Kerberos认证测试
 * @author	tokings.tang@embracesource.com
 * @date	2018年3月6日 下午2:22:10
 * @copyright	http://www.embracesource.com
 */
public class HiveWithKerberosTester {
	/**
	 * 用于连接Hive所需的一些参数设置 driverName:用于连接hive的JDBC驱动名 When connecting to
	 * HiveServer2 with Kerberos authentication, the URL format is:
	 * jdbc:hive2://&lt;host&gt;:&lt;port&gt;/&lt;db&gt;;principal=
	 * <Server_Principal_of_HiveServer2>
	 */
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	// 注意：这里的principal是固定不变的，其指的hive服务所对应的principal,而不是用户所对应的principal
	private static String url = "jdbc:hive2://192.168.1.211:10000/default;principal=hive/master.embracesource.com@EXAMPLE.COM";

	public static Connection getConnection() throws SQLException, ClassNotFoundException {

		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url);
		return conn;
	}

	/**
	 * 查看数据库下所有的表
	 *
	 * @param statement
	 * @return
	 */
	public static boolean showTables(Statement statement) {
		String sql = "SHOW TABLES";
		System.out.println("Running:" + sql);
		try {
			ResultSet res = statement.executeQuery(sql);
			System.out.println("执行“+sql+运行结果:");
			while (res.next()) {
				System.out.println(res.getString(1));
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 获取表的描述信息
	 *
	 * @param statement
	 * @param tableName
	 * @return
	 */
	public static boolean describTable(Statement statement, String tableName) {
		String sql = "DESCRIBE " + tableName;
		try {
			ResultSet res = statement.executeQuery(sql);
			System.out.println(tableName + "描述信息:");
			int count = res.getMetaData().getColumnCount();
			while (res.next()) {
				for (int i = 1; i <= count; i++) {
					System.out.print(res.getObject(i) + "\t");
				}
			}
			System.out.println();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 删除表
	 *
	 * @param statement
	 * @param tableName
	 * @return
	 */
	public static boolean dropTable(Statement statement, String tableName) {
		String sql = "DROP TABLE IF EXISTS " + tableName;
		System.out.println("Running:" + sql);
		try {
			statement.execute(sql);
			System.out.println(tableName + "删除成功");
			return true;
		} catch (SQLException e) {
			System.out.println(tableName + "删除失败");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 查看表数据
	 *
	 * @param statement
	 * @return
	 */
	public static boolean queryData(Statement statement, String tableName) {
		
		// 分页sql：select t.* from (select row_number() over (order by a) as rnum , test.* from test) t where t.rnum between 1 and 10;
		String sql = "SELECT * FROM " + tableName + " LIMIT 20";
		System.out.println("Running:" + sql);
		try {
			ResultSet res = statement.executeQuery(sql);
			System.out.println("执行结果:");
			int count = res.getMetaData().getColumnCount();
			while (res.next()) {
				for (int i = 1; i <= count; i++) {
					System.out.println(res.getObject(i) + "\t");
				}
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 创建表
	 *
	 * @param statement
	 * @return
	 */
	public static boolean createTable(Statement statement, String createTable, String srcTable) {
		
		String sql = "CREATE TABLE IF NOT EXISTS " + createTable + " AS SELECT * FROM " + srcTable;

		System.out.println("Running:" + sql);
		
		try {
			statement.execute(sql);
			System.out.println("执行成功！");
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * kerberos认证
	 */
	private static void kerberosAuthentication() {
		
		if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
			// 默认：这里不设置的话，win默认会到 C盘下读取krb5.init
			System.setProperty("java.security.krb5.conf", "krb5.ini");
		} else {
			// linux 会默认到 /etc/krb5.conf
			// 中读取krb5.conf,本文笔者已将该文件放到/etc/目录下，因而这里便不用再设置了
			System.setProperty("java.security.krb5.conf", "krb5.ini"); // for test req
		}
		
		/** 使用Hadoop安全登录 **/
		Configuration conf = new Configuration();
		// conf.set("hadoop.security.authentication", "kerberos");
		try {
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab("hive/master.embracesource.com@EXAMPLE.COM",
					"hive.service.keytab");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		kerberosAuthentication();
		
		Connection conn = null;
		
		try {
			 conn = getConnection();
			Statement stmt = conn.createStatement();

			showTables(stmt);
			
			String srcTableName = "test";
			String tableName = "test_kerberos";
			
			 createTable(stmt, tableName, srcTableName);
			
			 describTable(stmt, tableName);
			 
			 queryData(stmt, tableName);

//			 dropTable(stmt, tableName);
			
			 showTables(stmt);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
			
		System.out.println("测试结束.");
	}
}
