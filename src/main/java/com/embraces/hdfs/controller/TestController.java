package com.embraces.hdfs.controller;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.embraces.hdfs.service.TestService;

@RestController
@RequestMapping("/hdfs")
public class TestController {

	private  Logger log = LoggerFactory.getLogger(TestController.class);
	
	@Resource
	private TestService testService;
	
	@RequestMapping("/test")
	public Object test() {
		
		Object ret = testService.test();
		
		log.info("test ret:{}", ret);
		
		return ret;
	}
	
	@RequestMapping("/get")
	public Object get(@RequestParam String path) {
		
		Object ret = testService.get(path);
		
		log.info("get ret:{}", ret);
		
		return ret;
	}
	
	@RequestMapping("/put")
	public Object put(@RequestParam String path, @RequestParam String content) {
		
		Object ret = testService.put(path, content);
		
		log.info("put ret:{}", ret);
		
		return ret;
	}
	
	@RequestMapping("/append")
	public Object append(@RequestParam String path, @RequestParam String content) {
		
		Object ret = testService.apend(path, content);
		
		log.info("apend ret:{}", ret);
		
		return ret;
	}
	
	@RequestMapping("/delete")
	public Object delete(@RequestParam String path, @RequestParam String cascade) {
		
		Object ret = testService.delete(path, cascade);
		
		log.info("delete ret:{}", ret);
		
		return ret;
	}
}
