package com.exakaconsulting.spark.poc.config;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;

/**
 * Bean for SparkSessionManager
 * 
 * @author Toto
 *
 */
public class SparkSessionManager {
	
	private static final SparkSessionManager SPARK_SESSION_MANAGER = new SparkSessionManager();
	
	private static SparkSession sparkSession;
	
	private SparkSessionManager(){
		super();
	}
	
	public static SparkSessionManager getInstance(){
		return SPARK_SESSION_MANAGER;
	}
		
	public synchronized SparkSession getSparkSession(){
		
		// If sparSession has not been set --> retrieve it
		if (sparkSession == null){
			sparkSession = retrieveSparkSession();
		}
		
		return sparkSession;
		
	}
	
	private static SparkSession retrieveSparkSession() {
		
		ConfigurationParameters configParameters = ConfigurationParameters.getInstance();
		
		
		final SparkConf sparkConf = new SparkConf().setAppName("Test Karim")
		.setMaster(configParameters.getProperty(ConfigurationParameters.SPARK_MASTER_VALUE))
        .set("spark.executor.memory", configParameters.getProperty(ConfigurationParameters.SPARK_EXECUTOR_MEMORY))
        .set("spark.driver.memory", configParameters.getProperty(ConfigurationParameters.SPARK_DRIVER_MEMORY))
        .set("spark.executor.cores", configParameters.getProperty(ConfigurationParameters.SPARK_EXECUTOR_CORES))
        .set("spark.executor.instances", configParameters.getProperty(ConfigurationParameters.SPARK_EXECUTOR_INSTANCES))
        .set("spark.sql.shuffle.partitions", configParameters.getProperty(ConfigurationParameters.SPARK_SQL_SHUFFLE_PARTITIONS))
        .set("spark.serializer", KryoSerializer.class.getName())
        .set("spark.kryo.registrator", BatchTrafficStationRegistrator.class.getName())
        .set("spark.kryo.registrationRequired", "true")
        .set("spark.network.timeout", "10000001")
        .set("spark.executor.heartbeatInterval", "10000000")
        .set("spark.sql.autoBroadcastJoinThreshold", "52428800");
		
		return  SparkSession.builder().config(sparkConf).getOrCreate();
	}

	
	

}
