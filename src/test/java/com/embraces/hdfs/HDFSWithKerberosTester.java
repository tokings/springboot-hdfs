package com.embraces.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HDFS 带Kerberos认证测试
 * 
 * @author tokings.tang@embracesource.com
 * @date 2018年3月12日 下午5:21:18
 * @copyright http://www.embracesource.com
 */
public class HDFSWithKerberosTester {

	private static FileSystem fs = null;
	private static String desPath = "hdfs://master.embracesource.com:8020/tmp/test.txt";

	/**
	 * kerberos认证和初始化
	 */
	private static void kerberosAuthAndInit() {

		System.out.println("AUTHENTICATING ============================");
		
		// 初始化配置
		Configuration conf = new Configuration();

		if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
			// 设置KDC认证服务器连接配置文件
			if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
				// 默认：这里不设置的话，win默认会到 C盘下读取krb5.init
				System.setProperty("java.security.krb5.conf", "krb5.ini");
			} else {
				// linux 会默认到 /etc/krb5.conf
				System.setProperty("java.security.krb5.conf", "krb5.ini");
			}
		}

		try {
			// 用户认证
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab("hdfs-xyhdp@EXAMPLE.COM", "hdfs.headless.keytab");
			
			System.out.println("AUTHENTICATE END ============================");

			System.out.println("INITING ============================");
			// 实例化文件系统
			fs = FileSystem.get(conf);

			System.out.println("INIT END ============================");
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * 关闭文件系统
	 * @throws Exception
	 */
	private static void close() throws Exception {

		System.out.println("CLOSING ============================");
		
		// 使用之前，需要申请到HDFS服务端的资源，即需先初始化。
		if (fs == null) {
			kerberosAuthAndInit();
		}
		
		if(fs != null) {
			fs.close();
		}

		System.out.println("CLOSE END ============================");
	} 

	/**
	 * 写文件内容
	 * @throws Exception
	 */
	private static void write() throws Exception {
		
		// 使用之前，需要申请到HDFS服务端的资源，即需先初始化。
		if (fs == null) {
			kerberosAuthAndInit();
		}
		
		Path destPath = new Path(desPath);
		FSDataOutputStream os = null;
		
		try {
			System.out.println("WRITING ============================");
			
			byte[] buff = (new Date().toString() + "\n").getBytes();
			
			// 创建文件
			os = fs.create(destPath);
			
			// 写入数据
			os.write(buff, 0, buff.length);
			
			os.flush();

			System.out.println("WRITE END ============================");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 最终一定要释放资源
			if (os != null) {
				os.close();
			}
		}
	}
	
	/**
	 * 追加文件内容
	 * @throws Exception
	 */
	private static void append() throws Exception {
		
		// 使用之前，需要申请到HDFS服务端的资源，即需先初始化。
		if (fs == null) {
			kerberosAuthAndInit();
		}
		
		if(! "true".equalsIgnoreCase(fs.getConf().get("dfs.support.append"))) {
			System.out.println("Current DHFS not support append opt.");
			return;
		}
		
		Path destPath = new Path(desPath);
		FSDataOutputStream os = null;
		
		try {
			System.out.println("APPENDING ============================");
			
			byte[] buff = (new Date().toString() + "\n").getBytes();
			
			if(fs.exists(destPath)) {
				// 追加文件
				os = fs.append(destPath);
			} else {
				// 创建文件
				os = fs.create(destPath);
			}
			
			// 写入数据
			os.write(buff, 0, buff.length);
			
			os.flush();

			System.out.println("APPEND END ============================");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 最终一定要释放资源
			if (os != null) {
				os.close();
			}
		}
	}

	/**
	 * 读文件内容
	 * @throws Exception
	 */
	private static void read() throws Exception {

		// 使用之前，需要申请到HDFS服务端的资源，即需先初始化。
		if (fs == null) {
			kerberosAuthAndInit();
		}
		
		Path destPath = new Path(desPath);
		BufferedReader br = null;

		try {
			System.out.println("READING ============================");
			
			FSDataInputStream is = fs.open(destPath);
			br = new BufferedReader(new InputStreamReader(is));
			
			String content = null;
			while ((content = br.readLine()) != null) {
				System.out.println(content);
			}

			System.out.println("READ END ============================");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}
	
	/**
	 * 删除文件
	 * @throws Exception
	 */
	public static void delete() throws Exception {

		// 使用之前，需要申请到HDFS服务端的资源，即需先初始化。
		if (fs == null) {
			kerberosAuthAndInit();
		}
		
		Path destPath = new Path(desPath);

		try {
			System.out.println("DELETING ============================");
			
			boolean ret = fs.deleteOnExit(destPath);
			
			System.out.println("DELETE END ============================ ret:" + ret);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// TODO null implements
		}
	}

	public static void main(String[] args) {

		try {
			System.getProperties().forEach((k, v) -> System.out.println(k + ":" + v));

			delete();
			write();
			read();
//			append();
//			read();
//			delete();
			
			System.out.println("file exist:" + fs.exists(new Path(desPath)));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		System.out.println("测试结束.");
	}
}
