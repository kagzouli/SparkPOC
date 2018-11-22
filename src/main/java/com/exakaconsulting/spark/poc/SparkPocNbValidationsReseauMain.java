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

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.LongType;


public class SparkPocNbValidationsReseauMain {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Application test").setMaster("local[*]");

		SparkContext sparkContext = new SparkContext(sparkConf);

		SparkSession sparkSession = SparkSession.builder().appName("Application test").master("local").getOrCreate();

		// Groupage par nombre de validations 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				"D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy("LIBELLE_ARRET").sum("NB_VALD").orderBy("LIBELLE_ARRET");

		
		final DataFrameReader schemaDetailStation = constructDataFrameDetailStation(sparkSession);
		Dataset<Row> csvDetailStation = schemaDetailStation.format("csv").load(
				"D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\trafic-annuel-entrant-par-station-du-reseau-ferre-2017.csv");
		

		
		// final Dataset<Row> csvDataFrame =
		// dataFrameReader.format("csv").load("D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\signalisation-tricolore.csv");


		csvDetailStation.show(20);

		System.out.println("Spark context : " + csvDetailStation);

	}

	private static DataFrameReader constructDataFrameValidationBilStation(final SparkSession sparkSession) {

		final StructType schema = new StructType(
				new StructField[] { new StructField("JOUR", StringType, false, Metadata.empty()),
						new StructField("CODE_STIF_TRNS", IntegerType, false, Metadata.empty()),
						new StructField("CODE_STIF_RES", IntegerType, false, Metadata.empty()),
						new StructField("CODE_STIF_ARRET", IntegerType, false, Metadata.empty()),
						new StructField("LIBELLE_ARRET", StringType, false, Metadata.empty()),
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
				new StructField[] { new StructField("Rang", IntegerType, false, Metadata.empty()),
						new StructField("Reseau", StringType, false, Metadata.empty()),
						new StructField("Station", StringType, false, Metadata.empty()),
						new StructField("Trafic", LongType, false, Metadata.empty()),
						new StructField("Correspondance1", StringType, false, Metadata.empty()),
						new StructField("Correspondance2", StringType, false, Metadata.empty()),
						new StructField("Correspondance3", StringType, false, Metadata.empty()),
						new StructField("Correspondance4", StringType, false, Metadata.empty()),
						new StructField("Correspondance5", StringType, false, Metadata.empty()),
						new StructField("Ville", StringType, false, Metadata.empty()),
						new StructField("Arrondissement", IntegerType, false, Metadata.empty()),
				});

		
		final DataFrameReader dataFrameDetailStation = sparkSession.read();
		dataFrameDetailStation.option("header", "true").schema(schema).option("mode", "DROPMALFORMED")
				.option("delimiter", ";");

		return dataFrameDetailStation;

	}

}
