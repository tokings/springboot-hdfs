package com.embraces.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

/**
 * HDFS Java API操作启动类
 * @author	tokings.tang@embracesource.com
 * @date	2018年3月12日 下午5:04:28
 * @copyright	http://www.embracesource.com
 */
@ComponentScan(basePackages = { "com.embraces.hdfs"})
@EnableAutoConfiguration
public class HDFSAPP {
	
	public static void main(String[] args) {
		
		SpringApplication.run(HDFSAPP.class, args);
	}

//	@Bean
//	public MappingJackson2HttpMessageConverter customJackson2HttpMessageConverter() {
//		MappingJackson2HttpMessageConverter jsonConverter = new MappingJackson2HttpMessageConverter();
//		ObjectMapper objectMapper = PojoMapper.getObjectMapper();
//		jsonConverter.setObjectMapper(objectMapper);
//		return jsonConverter;
//	}
	
}
