package com.embraces.hdfs.dao;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository
public class TestDao extends BaseDao {
	
	private Logger log = LoggerFactory.getLogger(TestDao.class);
	
	public Object test() {
		
		String filePath = "/tmp/test.txt";
		Path path = new Path(filePath);

		boolean ret = false;
		FSDataOutputStream os = null;
		FSDataInputStream is = null;
		BufferedReader br = null;
		
		try {
			log.info("WRITING ============================");
			byte[] buff = "this is helloworld from java api!\n".getBytes();
			os = fs.create(path);
			os.write(buff, 0, buff.length);
			os.flush();

			log.info("READING ============================");
			is = fs.open(path);
			br = new BufferedReader(new InputStreamReader(is));
			String content = null;
			while ((content = br.readLine()) != null) {
				log.info(content);
			}
			ret = true;
		} catch (Exception e) {
			log.error("{}", e);;
		} finally {
			close(os);
			close(br);
			close(is);
		}
		
		log.info("ret: {}", ret);
		
		return ret;
	}

	public Object get(String filePath) {
		
		Path path = new Path(filePath);
		StringBuffer buf = new StringBuffer();
		FSDataInputStream is = null;
		BufferedReader br = null;
		
		try {
			log.info("READING ============================");
			is = fs.open(path);
			br = new BufferedReader(new InputStreamReader(is));
			String content = null;
			while ((content = br.readLine()) != null) {
				log.info(content);
				buf.append(content);
			}
		} catch (Exception e) {
			log.error("{}", e);;
		} finally {
			close(br);
			close(is);
		}
		
		log.info("ret size: {}", buf.length());
		
		return buf;
	}

	public Object put(String path, String content) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object apend(String path, String content) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object delete(String path, String cascade) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
