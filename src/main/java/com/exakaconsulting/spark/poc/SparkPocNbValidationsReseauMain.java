package com.exakaconsulting.spark.poc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConversions;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.LongType;

public class SparkPocNbValidationsReseauMain {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkPocNbValidationsReseauMain.class);


	/** Left outer **/
	private static final String LEFT_OUTER = "leftouter";
	
	private static final String DIRECTORY  = "D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources";
	
	/** columns **/
	private static final String STATION_COLUMN    = "STATION";
	private static final String RESEAU_COLUMN     = "RESEAU";
	private static final String VILLE_COLUMN      = "VILLE";
	private static final String ARRONDIS_COLUMN   = "ARRONDISSEMENT";
	private static final String NBRE_VALIDATION   = "NBRE_VALIDATION";
	

	public static void main(String[] args) {
		
		SparkConf sparkConf = retrieveSparkConf();
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

		// Groupage par nombre de validations 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				DIRECTORY + "\\validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy(STATION_COLUMN).sum("NB_VALD").withColumnRenamed("sum(NB_VALD)", NBRE_VALIDATION).orderBy(STATION_COLUMN);

		
		final DataFrameReader schemaDetailStation = constructDataFrameDetailStation(sparkSession);
		Dataset<Row> csvDetailStation = schemaDetailStation.format("csv").load(
				DIRECTORY + "\\trafic-annuel-entrant-par-station-du-reseau-ferre-2017.csv").select(RESEAU_COLUMN , STATION_COLUMN , VILLE_COLUMN , ARRONDIS_COLUMN);
		

		// Jointure avec les 2 datasets
		List<String> listColumns = Arrays.asList(STATION_COLUMN);
		Dataset<Row> csvJointure  = csvValidationBilStation.join(csvDetailStation, JavaConversions.asScalaBuffer(listColumns) ,LEFT_OUTER);
		
		
		//csvJointure.repartition(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(DIRECTORY + "\\output.csv");
		csvJointure.write().mode("overwrite").options(getDataOutputParamCsv()).csv(DIRECTORY + "\\output.csv");

		
		// csvValidationBilStation.show(20);
		// csvDetailStation.show(20);
		// csvJointure.show(20);

		LOGGER.info("Spark context : " + csvDetailStation);

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
	

	private static DataFrameReader constructDataFrameDetailStation(final SparkSession sparkSession) {

		final StructType schema = new StructType(
				new StructField[] { new StructField("RANG", IntegerType, false, Metadata.empty()),
						new StructField(RESEAU_COLUMN, StringType, false, Metadata.empty()),
						new StructField(STATION_COLUMN, StringType, false, Metadata.empty()),
						new StructField("TRAFFIC", LongType, false, Metadata.empty()),
						new StructField("CORRESPONDANCE1", StringType, false, Metadata.empty()),
						new StructField("CORRESPONDANCE2", StringType, false, Metadata.empty()),
						new StructField("CORRESPONDANCE3", StringType, false, Metadata.empty()),
						new StructField("CORRESPONDANCE4", StringType, false, Metadata.empty()),
						new StructField("CORRESPONDANCE5", StringType, false, Metadata.empty()),
						new StructField(VILLE_COLUMN, StringType, false, Metadata.empty()),
						new StructField(ARRONDIS_COLUMN, IntegerType, false, Metadata.empty()),
				});

		
		final DataFrameReader dataFrameDetailStation = sparkSession.read();
		dataFrameDetailStation.option("header", "true").schema(schema).option("mode", "DROPMALFORMED")
				.option("delimiter", ";");

		return dataFrameDetailStation;
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
