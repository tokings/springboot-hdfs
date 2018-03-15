package com.embraces.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class PhoenixWithoutKerberosTester {
	
	static final String URL = "jdbc:phoenix:192.168.1.211:2181:/hbase-unsecure";

	public static void main(String[] args) {
		Connection connection = null;
		Statement statement = null;
		
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			connection = DriverManager.getConnection(URL);
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
