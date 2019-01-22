package com.exakaconsulting.spark.poc;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.ListUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exakaconsulting.spark.poc.config.ConfigurationParameters;
import com.exakaconsulting.spark.poc.config.SparkSessionManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.collection.JavaConversions;

public class SparkPocNbValidationsRestCallReseauComplexeMain {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkPocNbValidationsRestCallReseauComplexeMain.class);


	/** Left outer **/
	private static final String LEFT_OUTER = "leftouter";
		
	/** columns **/
	private static final String STATION_COLUMN    = "station";
	private static final String NBRE_VALIDATION   = "nbreValidation";
	
	private static final String OVERRIDE_CSTE     = "overwrite";
		

	public static void main(String[] args) throws Exception{
		
		if (args == null || args.length != 1){
			throw new IllegalArgumentException("The programe must have one parameter, the directory");
		}
		
		final String directory = args[0];
		
		// Retrieve the SparkSession
		final SparkSession sparkSession = SparkSessionManager.getInstance().getSparkSession();

		
		// Groupage par nombre de validations et construction du Dataset correspondant. 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				directory + "/validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy(STATION_COLUMN).sum("NB_VALD").withColumnRenamed("sum(NB_VALD)", NBRE_VALIDATION).orderBy(STATION_COLUMN);

		
		List<String> listStation = new ArrayList<>();
		// collectAsList has to be avoided because it retrieves all the data to the driver.
		csvValidationBilStation.collectAsList().forEach(row -> listStation.add(row.getAs(STATION_COLUMN)));
		
		// Partition pour faire des appels packet de 10.
		List<List<String>> partitionListStation = ListUtils.partition(listStation, 10);
		
		final ObjectMapper objectMapper = new ObjectMapper();
		List<String> listRequestTableViewTemp = new ArrayList<>();
		for (List<String> partListStation : partitionListStation){
			CriteriaListNames criteriaListNames = new CriteriaListNames();
			criteriaListNames.setRequest(partListStation);
			
			
			final String criteriaRequestRest = objectMapper.writeValueAsString(criteriaListNames);
			listRequestTableViewTemp.add(criteriaRequestRest);
			
		}
		
		Dataset<String> datasetCritReq = sparkSession.createDataset(listRequestTableViewTemp, Encoders.STRING());
		datasetCritReq.createOrReplaceTempView("parametersinputpbl");
		
		final ConfigurationParameters configParameters = ConfigurationParameters.getInstance();
		
		Map<String, String> options = new HashMap<>();
		options.put("url", configParameters.getProperty(ConfigurationParameters.SPARK_URL_REST_COMPLEXE_CALL));
		options.put("securityToken", "TOKEN");
		options.put("input", "parametersinputpbl");
		options.put("method", "POST");
		
		final Dataset<Row> datasetResultCallRest = sparkSession.read().format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(options).load();
		
		// Select the column trafficsStationBean and get a Dataset of ListTrafficStationBean
		final Dataset<ListTrafficStationBean>  datasetListTrafficStationBean = datasetResultCallRest.select("trafficsStationBean").as(Encoders.bean(ListTrafficStationBean.class));
		
		// Get all the trafficStationBean from the bean and the dataset
		final Dataset<TrafficStationBean> datasetAllTraffic = datasetListTrafficStationBean.flatMap((FlatMapFunction<ListTrafficStationBean, TrafficStationBean>) listTrafficStationBean -> listTrafficStationBean.getTrafficsStationBean().iterator(), Encoders.bean(TrafficStationBean.class));
		
				
		
		if (LOGGER.isInfoEnabled()){
			LOGGER.info(String.format("The number of data is %s", datasetAllTraffic.count()));
		}


		// Jointure avec les 2 datasets
		List<String> listColumns = Arrays.asList(STATION_COLUMN);
		final Dataset<Row> csvJointure  = csvValidationBilStation.join(datasetAllTraffic, JavaConversions.asScalaBuffer(listColumns) ,LEFT_OUTER).orderBy(STATION_COLUMN)
				.withColumn("listCorrespondance", functions.to_json(functions.struct(functions.col("listCorrespondance"))));
		
		
		// Write to an output file.
		if (directory.contains("hdfs://")){
			csvJointure.rdd().saveAsTextFile(directory + "/outputfile.csv");
			csvJointure.coalesce(1).write().mode(OVERRIDE_CSTE).options(getDataOutputParamCsv()).csv(directory + "/output.csv");

		}else{
			csvJointure.write().mode(OVERRIDE_CSTE).options(getDataOutputParamCsv()).csv(directory + "/output.csv");
			csvJointure.coalesce(1).write().mode(OVERRIDE_CSTE).options(getDataOutputParamCsv()).csv(directory + "/outputfinal.csv");

		}
		
		
		csvJointure.show(4000);
		
		
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
	
	
	 public static Map<String, String> getDataOutputParamCsv() {
	        Map<String, String> options = new HashMap<>();
	        options.put("header", "true");
	        options.put("delimiter", ";");
	        options.put("comment", "#");
	        return options;
	    }


}
