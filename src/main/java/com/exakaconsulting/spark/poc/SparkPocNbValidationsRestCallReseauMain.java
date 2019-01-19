package com.exakaconsulting.spark.poc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.collection.JavaConversions;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import org.apache.spark.sql.functions;

public class SparkPocNbValidationsRestCallReseauMain {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkPocNbValidationsRestCallReseauMain.class);


	/** Left outer **/
	private static final String LEFT_OUTER = "leftouter";
		
	/** columns **/
	private static final String STATION_COLUMN    = "station";
	private static final String NBRE_VALIDATION   = "nbreValidation";
		

	public static void main(String[] args) throws Exception{
		
		if (args == null || args.length != 1){
			throw new IllegalArgumentException("The programe must have one parameter, the directory");
		}
		
		final String directory = args[0];
		
		SparkConf sparkConf = retrieveSparkConf();
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

		// Groupage par nombre de validations 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				directory + "/validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy(STATION_COLUMN).sum("NB_VALD").withColumnRenamed("sum(NB_VALD)", NBRE_VALIDATION).orderBy(STATION_COLUMN);

		
		List<String> listStation = new ArrayList<>();
		csvValidationBilStation.collectAsList().forEach(row -> listStation.add(row.getAs(STATION_COLUMN)));
		
		
		List<List<String>> partitionListStation = ListUtils.partition(listStation, 10);
		
		List<String> listRequestTableViewTemp = new ArrayList<>();
		final ObjectMapper objectMapper = new ObjectMapper();
		for (List<String> partListStation : partitionListStation){
			CriteriaListNames criteriaListNames = new CriteriaListNames();
			criteriaListNames.setRequest(partListStation);
			
			
			final String criteriaRequestRest = objectMapper.writeValueAsString(criteriaListNames);
			listRequestTableViewTemp.add(criteriaRequestRest);
			
		}
		
		Dataset<String> datasetCritReq = sparkSession.createDataset(listRequestTableViewTemp, Encoders.STRING());
		datasetCritReq.createOrReplaceTempView("parametersinputpbl");
		
		
		Map<String, String> options = new HashMap<>();
		options.put("url", "http://DESKTOP-M49AV3O:8080/StationDemoWebSparkRest/statioNamesn/findStationsByNames");
		options.put("securityToken", "TOKEN");
		options.put("input", "parametersinputpbl");
		options.put("method", "POST");
		
		Dataset<Row> datasetResultCallRest = sparkSession.read().format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(options).load();
		datasetResultCallRest.printSchema();
		
		datasetResultCallRest.show(2000);
		
		
		LOGGER.info(String.format("The number of data is %s", datasetResultCallRest.count()));

		
		

		// Jointure avec les 2 datasets
		List<String> listColumns = Arrays.asList(STATION_COLUMN);
		Dataset<Row> csvJointure  = csvValidationBilStation.join(datasetResultCallRest, JavaConversions.asScalaBuffer(listColumns) ,LEFT_OUTER).orderBy(STATION_COLUMN)
				.withColumn("listCorrespondance", functions.to_json(functions.struct(functions.col("listCorrespondance"))));
		
		
		csvJointure.show(20000);
		
		// csvJointure.repartition(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(DIRECTORY + "/output.csv");
		
		if (directory.contains("hdfs://")){
			csvJointure.rdd().saveAsTextFile(directory + "/outputfile.csv");
			csvJointure.coalesce(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(directory + "/output.csv");

		}else{
			csvJointure.write().mode("overwrite").options(getDataOutputParamCsv()).csv(directory + "/output.csv");
			csvJointure.coalesce(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(directory + "/outputfinal.csv");

		}
		
		
		// csvJointure.show(4000);
		
		
		// LOGGER.info("Spark context : " + csvDetailStation);

	}

	private static DataFrameReader constructDataFrameValidationBilStation(final SparkSession sparkSession) {

		final StructType schema = new StructType(
				new StructField[] { new StructField("JOUR", StringType, false, Metadata.empty()),
						new StructField("CODE_STIF_TRNS", IntegerType, false, Metadata.empty()),
						new StructField("CODE_STIF_RES", IntegerType, false, Metadata.empty()),
						new StructField("CODE_STIF_ARRET", IntegerType, false, Metadata.empty()),
						new StructField(STATION_COLUMN, StringType, false, Metadata.empty()),
						new StructField("ID_REFA_LDA", IntegerType, false, Metadata.empty()),
						new StructField("CATEGORIE_TITRE", StringType, false, Metadata.empty()),
						new StructField("NB_VALD", IntegerType, false, Metadata.empty()), });

		final DataFrameReader schemaValidationBilStation = sparkSession.read();
		schemaValidationBilStation.option("header", "true").schema(schema).option("mode", "DROPMALFORMED")
				.option("delimiter", ";");

		return schemaValidationBilStation;

	}
	

	
	private static SparkConf retrieveSparkConf() {
		
		ConfigurationParameters configParameters = ConfigurationParameters.getInstance();
		
		
		SparkConf sparkConf = new SparkConf().setAppName("Test Karim")
		.setMaster(configParameters.getProperty(ConfigurationParameters.SPARK_MASTER_VALUE))
        .set("spark.executor.memory", configParameters.getProperty(ConfigurationParameters.SPARK_EXECUTOR_MEMORY))
        .set("spark.driver.memory", configParameters.getProperty(ConfigurationParameters.SPARK_DRIVER_MEMORY))
        .set("spark.executor.cores", configParameters.getProperty(ConfigurationParameters.SPARK_EXECUTOR_CORES))
        .set("spark.executor.instances", configParameters.getProperty(ConfigurationParameters.SPARK_EXECUTOR_INSTANCES))
        .set("spark.sql.shuffle.partitions", configParameters.getProperty(ConfigurationParameters.SPARK_SQL_SHUFFLE_PARTITIONS))
        .set("spark.serializer", KryoSerializer.class.getName())
        .set("spark.kryo.registrator", BatchTestKarimRegistrator.class.getName())
        .set("spark.kryo.registrationRequired", "true")
        .set("spark.network.timeout", "10000001")
        .set("spark.executor.heartbeatInterval", "10000000")
        .set("spark.sql.autoBroadcastJoinThreshold", "52428800");
		return sparkConf;
	}
	
	 public static Map<String, String> getDataOutputParamCsv() {
	        Map<String, String> options = new HashMap<>();
	        options.put("header", "true");
	        options.put("delimiter", ";");
	        options.put("comment", "#");
	        return options;
	    }


}
