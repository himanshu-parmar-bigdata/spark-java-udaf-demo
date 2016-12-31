package com.parmarh.driver;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.parmarh.pojo.UserActivities;
import com.parmarh.udf.CustomUserAggUDF;

public class UserAggDriver {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().config(new SparkConf())// .set("spark.executor.memory","5g").
																					// .setMaster("yarn").set("spark.driver.memory","4g"))
				.getOrCreate();

		Dataset<UserActivities> inuptDS = sparkSession.read().option("header", "true")
				.csv("s3://mybucket/input/user_activities.txt").as(Encoders.bean(UserActivities.class));

		sparkSession.udf().register("customAgg", new CustomUserAggUDF());
		
		// for debugging
		inuptDS.show();

		Dataset userAndDeviceAgg = inuptDS.groupBy(inuptDS.col("swid"), inuptDS.col("device_category")).agg(
				sum(inuptDS.col("time_spent")).as("time_spent"), sum(inuptDS.col("video_start")).as("video_start"));

		// for debugging
		userAndDeviceAgg.printSchema();

		Dataset udfOutput = userAndDeviceAgg.groupBy(userAndDeviceAgg.col("swid"))
				.agg(callUDF("customAgg", userAndDeviceAgg.col("device_category"), userAndDeviceAgg.col("time_spent"),
						userAndDeviceAgg.col("video_start")).as("multicolumn"));

		// Schema of this udfOutput dataset would be
		// root
		// |-- swid: string (nullable = true)
		// |-- multicolumn: struct (nullable = true)
		// | |-- device_distribution: string (nullable = true)
		// | |-- time_spent: long (nullable = true)
		// | |-- video_start: long (nullable = true)
		udfOutput.printSchema();

		// for debugging
		Object[] arr = (Object[]) udfOutput.take(5);
		for (int i = 0; i < arr.length; i++) {
			System.out.println(arr[i]);
		}

		// generating new columns by splitting a column returned by
		// UDF(multicolumn)
		Dataset output = udfOutput.withColumn("device_distribution", col("multicolumn.device_distribution"))
				.withColumn("time_spent", col("multicolumn.time_spent"))
				.withColumn("video_start", col("multicolumn.video_start")).drop("multicolumn");

		// for debugging
		Object[] arr2 = (Object[]) output.take(5);
		for (int i = 0; i < arr2.length; i++) {
			System.out.println(arr2[i]);
		}

		// write final dataset to HDFS or S3 as a AVRO in Parquet file format
		output
		.coalesce(1)
		.write().mode(SaveMode.Overwrite)
		.format("com.databricks.spark.avro")
		.parquet("s3://mybucket/output");
	}

}
