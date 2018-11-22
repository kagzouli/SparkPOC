package com.exakaconsulting.spark.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.LongType;


public class SparkPocNbValidationsReseauMain {
	
	/** Left outer **/
	private static final String LEFT_OUTER = "leftouter";
	
	/** columns **/
	private static final String STATION_COLUMN    = "STATION";
	private static final String RESEAU_COLUMN     = "RESEAU";
	private static final String VILLE_COLUMN      = "VILLE";
	private static final String ARRONDIS_COLUMN   = "ARRONDISSEMENT";

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Application test").setMaster("local[*]");

		SparkContext sparkContext = new SparkContext(sparkConf);

		SparkSession sparkSession = SparkSession.builder().appName("Application test").master("local").getOrCreate();

		// Groupage par nombre de validations 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				"D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy(STATION_COLUMN).sum("NB_VALD").orderBy(STATION_COLUMN);

		
		final DataFrameReader schemaDetailStation = constructDataFrameDetailStation(sparkSession);
		Dataset<Row> csvDetailStation = schemaDetailStation.format("csv").load(
				"D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\trafic-annuel-entrant-par-station-du-reseau-ferre-2017.csv");
		

		// Jointure avec les 2 datasets
		List<String> listColumns = Arrays.asList(STATION_COLUMN);
		Dataset<Row> csvJointure  = csvValidationBilStation.join(csvDetailStation, JavaConversions.asScalaBuffer(listColumns) ,LEFT_OUTER);
		


		csvDetailStation.show(20);

		System.out.println("Spark context : " + csvDetailStation);

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

}
