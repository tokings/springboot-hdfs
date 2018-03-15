package com.embraces.hdfs.service;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.embraces.hdfs.dao.TestDao;

@Service
public class TestService {

	private Logger log = LoggerFactory.getLogger(TestService.class);

	@Resource
	private TestDao testDao;
	
	public Object test() {
		
		Object ret = testDao.test();

		log.info("ret:{}", ret);
		
		return ret;
	}

	public Object get(String path) {
		
		Object ret = testDao.get(path);

		log.info("get ret:{}", ret);
		
		return ret;
	}

	public Object put(String path, String content) {
		
		Object ret = testDao.put(path, content);

		log.info("put ret:{}", ret);
		
		return ret;
	}

	public Object apend(String path, String content) {
		
		Object ret = testDao.apend(path, content);

		log.info("apend ret:{}", ret);
		
		return ret;
	}

	public Object delete(String path, String cascade) {
		
		Object ret = testDao.delete(path, cascade);

		log.info("delete ret:{}", ret);
		
		return ret;
	}
}
