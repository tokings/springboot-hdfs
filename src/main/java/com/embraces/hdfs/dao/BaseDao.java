package com.embraces.hdfs.dao;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository
public class BaseDao {
	
	Logger log = LoggerFactory.getLogger(BaseDao.class);
	
	private static Configuration conf = new Configuration();
	protected static FileSystem fs = null;

	@PostConstruct
	public void init() throws Exception {
		fs = FileSystem.get(conf);
	}
	
	protected void close(Closeable c) {
		if(c != null) {
			try {
				c.close();
			} catch (IOException e) {
				c = null;
				log.error("{}", e);
			}
		}
	}
}
