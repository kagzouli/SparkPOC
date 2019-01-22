package com.exakaconsulting.spark.poc;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exakaconsulting.spark.poc.config.SparkSessionManager;

import scala.collection.JavaConversions;

public class SparkPocNbValidationsReseauMain {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkPocNbValidationsReseauMain.class);


	/** Left outer **/
	private static final String LEFT_OUTER = "leftouter";
		
	/** columns **/
	private static final String STATION_COLUMN    = "STATION";
	private static final String RESEAU_COLUMN     = "RESEAU";
	private static final String VILLE_COLUMN      = "VILLE";
	private static final String ARRONDIS_COLUMN   = "ARRONDISSEMENT";
	private static final String NBRE_VALIDATION   = "NBRE_VALIDATION";
	
	private static final String HEADER_CSTE       = "header";
	private static final String DELIMITER_CSTE    = "delimiter";
	private static final String OVERRIDE_CSTE     = "overwrite";
	

	public static void main(String[] args) {
		
		if (args == null || args.length != 1){
			throw new IllegalArgumentException("The programe must have one parameter, the directory");
		}
		
		final String directory = args[0];
		
		// Retrieve the SparkSession from the manager
		final SparkSession sparkSession = SparkSessionManager.getInstance().getSparkSession();

		// Groupage par nombre de validations 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				directory + "/validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy(STATION_COLUMN).sum("NB_VALD").withColumnRenamed("sum(NB_VALD)", NBRE_VALIDATION).orderBy(STATION_COLUMN);

		
		final DataFrameReader schemaDetailStation = constructDataFrameDetailStation(sparkSession);
		Dataset<Row> csvDetailStation = schemaDetailStation.format("csv").load(
				directory + "/trafic-annuel-entrant-par-station-du-reseau-ferre-2017.csv").select(RESEAU_COLUMN , STATION_COLUMN , VILLE_COLUMN , ARRONDIS_COLUMN);
		

		if (LOGGER.isInfoEnabled()){
			LOGGER.info(String.format("The number of data is %s", csvDetailStation.count()));			
		}
		
		csvDetailStation.show(400);
		
		// Jointure avec les 2 datasets
		List<String> listColumns = Arrays.asList(STATION_COLUMN);
		Dataset<Row> csvJointure  = csvValidationBilStation.join(csvDetailStation, JavaConversions.asScalaBuffer(listColumns) ,LEFT_OUTER).orderBy(STATION_COLUMN);
		
				
		if (directory.contains("hdfs://")){
			csvJointure.rdd().saveAsTextFile(directory + "/outputfile.csv");
			csvJointure.coalesce(1).write().mode(OVERRIDE_CSTE).options(getDataOutputParamCsv()).csv(directory + "/output.csv");

		}else{
			csvJointure.write().mode(OVERRIDE_CSTE).options(getDataOutputParamCsv()).csv(directory + "/output.csv");
			csvJointure.coalesce(1).write().mode(OVERRIDE_CSTE).options(getDataOutputParamCsv()).csv(directory + "/outputfinal.csv");

		}
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
		schemaValidationBilStation.option(HEADER_CSTE, "true").schema(schema).option("mode", "DROPMALFORMED")
				.option(DELIMITER_CSTE, ";");

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
		dataFrameDetailStation.option(HEADER_CSTE, "true").schema(schema).option("mode", "DROPMALFORMED")
				.option(DELIMITER_CSTE, ";");

		return dataFrameDetailStation;
	}
		
	 public static Map<String, String> getDataOutputParamCsv() {
	        Map<String, String> options = new HashMap<>();
	        options.put(HEADER_CSTE, "true");
	        options.put(DELIMITER_CSTE, ";");
	        options.put("comment", "#");
	        return options;
	    }


}
