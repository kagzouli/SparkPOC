package com.exakaconsulting.spark.poc;

import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats;
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary;
import org.apache.spark.sql.execution.datasources.WriteTaskResult;
import org.apache.spark.sql.execution.joins.UnsafeHashedRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

public class BatchTestKarimRegistrator implements KryoRegistrator {
	
	/** Logger **/
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchTestKarimRegistrator.class);


	@Override
	public void registerClasses(Kryo kryo) {
		kryo.register(byte[][].class);
		kryo.register(UnsafeHashedRelation.class);
		kryo.register(InternalRow[].class);
		kryo.register(UnsafeRow.class);
		kryo.register(WriteTaskResult.class);
		kryo.register(ExecutedWriteSummary.class);
		kryo.register(BasicWriteTaskStats.class);
		kryo.register(org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering.class);
		kryo.register(org.apache.spark.sql.catalyst.expressions.SortOrder[].class);
		kryo.register(org.apache.spark.sql.catalyst.expressions.SortOrder.class);
		kryo.register(org.apache.spark.sql.catalyst.expressions.BoundReference.class);
		kryo.register(org.apache.spark.sql.catalyst.InternalRow.class);
		kryo.register(org.apache.spark.sql.types.StructType.class);
		kryo.register(org.apache.spark.sql.types.StructField[].class);
		kryo.register(org.apache.spark.sql.types.StructField.class);
		kryo.register(org.apache.spark.sql.types.Metadata.class);
		kryo.register(org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class);
		kryo.register(org.apache.spark.sql.types.LongType$.class);
		kryo.register(org.apache.spark.sql.types.ArrayType.class);
		
		
		try {
			kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"));
			kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"));
			
			kryo.register(java.lang.Class.class);
		//	kryo.register(Class.forName("org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult"));
			kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"));
			kryo.register(Class.forName("scala.math.Ordering$$anon$4"));
			kryo.register(Class.forName("org.apache.spark.sql.catalyst.InternalRow$$anonfun$getAccessor$8"));
			kryo.register(org.apache.spark.sql.types.StringType$.class);
			kryo.register(org.apache.spark.sql.catalyst.trees.Origin.class);
			kryo.register(org.apache.spark.sql.catalyst.expressions.Ascending$.class);
			kryo.register(org.apache.spark.sql.catalyst.expressions.NullsFirst$.class);
		} catch (ClassNotFoundException e) {
			LOGGER.error("Error during kryo registration!", e);
		}
		
	}

}
