package com.embraces.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Phoenix开启Kerberos认证测试
 * @author	tokings.tang@embracesource.com
 * @date	2018年3月5日 下午1:42:27
 * @copyright	http://www.embracesource.com
 */
public class PhoenixWithKerberosTester {
	
	/**
	 * Phoenix连接URL
	 * <br><font color="red" style="font-weight:bold;">格式：</font>
	 * <br>jdbc:phoenix:hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent:principalName:keytabLocalPath
	 * <br>
	 * <br><font color="red" style="font-weight:bold;">说明：</font>
	 * <br>hbase.zookeeper.quorum:zk集群主机IP地址列表
	 * <br>hbase.zookeeper.property.clientPort:zk集群端口（默认2181，无预设值）
	 * <br>hbase.zookeeper.znode.parent:hbase在zk集群的根路径（默认路径/hbase-secure，无预设值）
	 * <br>principalName:hbase服务的principal名称（默认user-domain@realm），可通过集群控制台HBase服务的配置Tab页查看
	 * <br>keytabLocalPath:hbase服务的keytab文件路径（默认/etc/security/keytabs/hbase.headless.keytab），可通过集群控制台HBase服务的配置Tab页查看。
	 * 需要复制到本地环境，指定对应的绝对路径和相对路径（Window系统推荐，因为Window系统盘符带“:”，会干扰URL解析导致失败）
	 * <br>
	 * <br><font color="red" style="font-weight:bold;">注意：</font>
	 * <br>（1）需要在本机环境配置好Kerberos访问配置文件（默认配置在：%JAVA_HOME%\lib\security\java.security.krb5.conf）。
	 * 也可以在系统中设置系统属性：java.security.krb5.conf指定文件路径（一般放入到类路径然后指定属性即可）。
	 * 示例：-Djava.security.krb5.conf=krb5.ini或System.setProperty("java.security.krb5.conf", "krb5.ini")。
	 * <br>（2）需要复制hbase-site.xml和core-site.xml（需要用到hadoop.security.authentication属性，也可以将此属性写入到hbase-site.xml文件当中，然后删除core-site.xml，但不推荐）到类路径。
	 */
	static final String URL = "jdbc:phoenix:ict-nn-02.cars.com,ict-nn-01.cars.com,ict-cn-01.cars.com:2181:/hbase-secure:hbase-ictcluster@CARS.COM:hbase.headless.keytab";

	public static void main(String[] args) {
		Connection connection = null;
		Statement statement = null;
		// 设置Kerberos服务器连接所使用的配置文件路径（或者通过虚拟机参数-Djava.security.krb5.conf=krb5.ini指定）
//		System.setProperty("java.security.krb5.conf", "krb5.ini");
		System.out.println(System.getProperty("java.security.krb5.conf"));
		
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(URL);
//			connection = DriverManager.getConnection(URL);
			connection.setAutoCommit(true);
			
			statement = connection.createStatement();
			
			ResultSet rs = statement.executeQuery("select * from test.tbl_1");
			while(rs.next()) {
				int cnt = rs.getMetaData().getColumnCount();
				for (int j = 0; j < cnt; j++) {
					System.out.print(rs.getObject(j + 1) + "\t");
					System.out.println();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
				statement.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
