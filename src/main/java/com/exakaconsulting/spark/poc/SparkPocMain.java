package com.exakaconsulting.spark.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkPocMain {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("Application test").setMaster("local[*]");
		
		SparkContext sparkContext = new SparkContext(sparkConf);
		
		
		
		

		
		SparkSession sparkSession = SparkSession
			    .builder()
			    .appName("Application test").master("local")
			    .getOrCreate();
		
		
		final DataFrameReader dataFrameReader = sparkSession.read();
		dataFrameReader.option("header", "true")
		.schema(constructStructType())
		.option("mode", "DROPMALFORMED")
		.option("delimiter", ";");
		
		Dataset<Row> csvDataFrame  = dataFrameReader.format("csv").load("D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		//final Dataset<Row> csvDataFrame  = dataFrameReader.format("csv").load("D:\\Karim\\dev\\workspace\\SparkPOC\\SparkPOC\\src\\main\\resources\\signalisation-tricolore.csv");
		
		csvDataFrame = csvDataFrame.groupBy("LIBELLE_ARRET").sum("NB_VALD").orderBy("LIBELLE_ARRET");
		
		csvDataFrame.registerTempTable("tempTable");
		csvDataFrame.sqlContext().sql("select * from tempTable").show();
				
				
		System.out.println("Spark context : "+ csvDataFrame );

	}
	
	private static StructType constructStructType(){
		final StructType schema = new StructType(new StructField[]{
                new StructField("JOUR", StringType, false, Metadata.empty()),
                new StructField("CODE_STIF_TRNS", IntegerType, false, Metadata.empty()),
                new StructField("CODE_STIF_RES", IntegerType, false, Metadata.empty()),
                new StructField("CODE_STIF_ARRET", IntegerType, false, Metadata.empty()),
                new StructField("LIBELLE_ARRET", StringType, false, Metadata.empty()),
                new StructField("ID_REFA_LDA", IntegerType, false, Metadata.empty()),
                new StructField("CATEGORIE_TITRE", StringType, false, Metadata.empty()),
                new StructField("NB_VALD", IntegerType, false, Metadata.empty()),
        });
		return schema;


	}

}
