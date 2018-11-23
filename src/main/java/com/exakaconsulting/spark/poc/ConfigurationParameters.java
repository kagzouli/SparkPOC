package com.exakaconsulting.spark.poc;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationParameters {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationParameters.class);

	
	private static final ConfigurationParameters INSTANCE = new ConfigurationParameters();
	
	private Properties properties    = new Properties();
	
	/** Config spark **/
	public static final String  SPARK_EXECUTOR_MEMORY          = "spark.executor.memory";
	public static final String  SPARK_DRIVER_MEMORY            = "spark.driver.memory";
	public static final String  SPARK_EXECUTOR_CORES           = "spark.executor.cores";
	public static final String  SPARK_EXECUTOR_INSTANCES       = "spark.executor.instances";
	public static final String  SPARK_SQL_SHUFFLE_PARTITIONS   = "spark.sql.shuffle.partitions";
	
	private static final String CONFIG_FILE         = "CONFIG_FILE";


	
	
	private ConfigurationParameters(){
		super();
		this.initializeProperties();
	}
	
	public static ConfigurationParameters getInstance(){
		return INSTANCE;
	}
	
	
	private synchronized void initializeProperties(){
		final String configFile = System.getProperty(CONFIG_FILE);
		
		if (StringUtils.isBlank(configFile)){
			throw new IllegalArgumentException(String.format("the JVM variable '%s' must be set", CONFIG_FILE));
		}
		
		File file = new File(configFile);
		
		if (!file.exists() || !file.isFile() || !file.canRead()){
			throw new IllegalArgumentException(String.format("'%s' must be a readable file", configFile));
		}
		
		try {
			this.properties.load(new FileInputStream(file));
		} catch (Exception exception) {
			LOGGER.error(exception.getMessage() , exception);
			throw new IllegalArgumentException(exception);
		}
	}
	
	
	public String getProperty(final String key){
		return this.properties.getProperty(key);
	}

}
