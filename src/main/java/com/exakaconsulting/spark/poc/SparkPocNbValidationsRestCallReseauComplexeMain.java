package com.exakaconsulting.spark.poc;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.ListUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
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
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

public class SparkPocNbValidationsRestCallReseauComplexeMain {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(SparkPocNbValidationsRestCallReseauComplexeMain.class);


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

		
		/* |-- trafficsStationBean: array (nullable = true)
		 |    |-- element: struct (containsNull = true)
		 |    |    |-- arrondissement: long (nullable = true)
		 |    |    |-- id: long (nullable = true)
		 |    |    |-- listCorrespondance: array (nullable = true)
		 |    |    |    |-- element: string (containsNull = true)
		 |    |    |-- reseau: string (nullable = true)
		 |    |    |-- station: string (nullable = true)
		 |    |    |-- traffic: long (nullable = true)
		 |    |    |-- ville: string (nullable = true)
		*/
		StructType schemaBean = new StructType(new StructField[] {
				new StructField("arrondissement",DataTypes.LongType, true,Metadata.empty()),
				new StructField("id", DataTypes.LongType, true,Metadata.empty()),
				new StructField("listCorrespondance",DataTypes.createArrayType(DataTypes.StringType), true,Metadata.empty()),
				new StructField("reseau",DataTypes.StringType, true,Metadata.empty()),
				new StructField("station",DataTypes.StringType, true,Metadata.empty()),
				new StructField("traffic",DataTypes.LongType, true,Metadata.empty()),
				new StructField("ville",DataTypes.StringType, true,Metadata.empty()),
				
		});
		
		
		StructType schemaGlobal = new StructType(new StructField[] {
				new StructField("element",DataTypes.createArrayType(schemaBean), true,
						Metadata.empty()) });
		
		// Groupage par nombre de validations 
		final DataFrameReader schemaValidationBilStation = constructDataFrameValidationBilStation(sparkSession);
		Dataset<Row> csvValidationBilStation = schemaValidationBilStation.format("csv").load(
				directory + "/validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
		csvValidationBilStation = csvValidationBilStation.groupBy(STATION_COLUMN).sum("NB_VALD").withColumnRenamed("sum(NB_VALD)", NBRE_VALIDATION).orderBy(STATION_COLUMN);

		
		List<String> listStation = new ArrayList<>();
		// collectAsList has to be avoided because it retrieves all the data to the driver.
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
		
		final ConfigurationParameters configParameters = ConfigurationParameters.getInstance();
		
		Map<String, String> options = new HashMap<>();
		options.put("url", configParameters.getProperty(ConfigurationParameters.SPARK_URL_REST_COMPLEXE_CALL));
		options.put("securityToken", "TOKEN");
		options.put("input", "parametersinputpbl");
		options.put("method", "POST");
		
		Dataset<Row> datasetResultCallRest = sparkSession.read().format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(options).load();
		datasetResultCallRest.printSchema();
		
		final Dataset<Row> datasetResultCallFiltered = datasetResultCallRest.select("trafficsStationBean");
		Dataset<ListTrafficStationBean>  listTrafficStationBean = datasetResultCallFiltered.as(Encoders.bean(ListTrafficStationBean.class));
		List<ListTrafficStationBean> datasetTraffic = listTrafficStationBean.collectAsList();
		
		List<TrafficStationBean> listFinal = new ArrayList<>();
		
		for (ListTrafficStationBean listTemp : datasetTraffic){
			listFinal.addAll(listTemp.getTrafficsStationBean());
		}
		
				
		
		LOGGER.info(String.format("The number of data is %s", datasetResultCallRest.count()));
		LOGGER.info(String.format("The number final of data is %s", listFinal.size()));

		
		final Dataset<Row> datasetAllTraffic = sparkSession.createDataFrame(listFinal, TrafficStationBean.class);

		// Jointure avec les 2 datasets
		List<String> listColumns = Arrays.asList(STATION_COLUMN);
		Dataset<Row> csvJointure  = csvValidationBilStation.join(datasetAllTraffic, JavaConversions.asScalaBuffer(listColumns) ,LEFT_OUTER).orderBy(STATION_COLUMN)
				.withColumn("listCorrespondance", functions.to_json(functions.struct(functions.col("listCorrespondance"))));
		
		
	//	csvJointure.show(20000);
		
		// csvJointure.repartition(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(DIRECTORY + "/output.csv");
		
		if (directory.contains("hdfs://")){
			csvJointure.rdd().saveAsTextFile(directory + "/outputfile.csv");
			csvJointure.coalesce(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(directory + "/output.csv");

		}else{
			csvJointure.write().mode("overwrite").options(getDataOutputParamCsv()).csv(directory + "/output.csv");
			csvJointure.coalesce(1).write().mode("overwrite").options(getDataOutputParamCsv()).csv(directory + "/outputfinal.csv");

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
