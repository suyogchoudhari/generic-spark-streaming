package com.sparktest.job;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

public class AbstractJob {

	private static Logger logger = Logger.getLogger(StreamingJob.class);
	
	protected static Map<String,Object> config =  null;
	
	public static void loadConifg() throws IOException{
		String resourceName = "config.yml";
        try( InputStream in = AbstractJob.class.getClassLoader().getResourceAsStream(resourceName)) {
        	
        	Yaml yaml = new Yaml();
        	config = (Map<String, Object>) yaml.load(in);
        	logger.info(config);
        }
	}
}
