package com.parmarh.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.gson.Gson;

import scala.Tuple3;

public class CustomUserAggUDF extends UserDefinedAggregateFunction {

	/**
	 * Input Data Type Schema
	 */
	@Override
	public StructType inputSchema() {
		return new StructType().add("device_category", DataTypes.StringType).add("time_spent", DataTypes.LongType)
				.add("video_start", DataTypes.LongType);
	}

	/**
	 * Intermediate Schema
	 */
	@Override
	public StructType bufferSchema() {
		return new StructType().add("device_category", DataTypes.StringType).add("time_spent", DataTypes.LongType)
				.add("video_start", DataTypes.LongType);

	}

	/**
	 * Datatype to be returned from UDF
	 */
	@Override
	public DataType dataType() {

		return new StructType().
				// add("device_distr",
				// DataTypes.createArrayType(DataTypes.createMapType(DataTypes.StringType,
				// DataTypes.FloatType, true)))
		add("device_distribution", DataTypes.StringType).add("time_spent", DataTypes.LongType).add("video_start",
				DataTypes.LongType);

	}

	/**
	 * If output is deterministic all the time for same input
	 */
	@Override
	public boolean deterministic() {
		return true;
	}

	/**
	 * Called after all the entries are exhausted.
	 */
	@Override
	public Object evaluate(Row buffer) {
		long totalTimeSpent = buffer.getLong(1);

		String userDeviceString = buffer.getString(0);

		String[] userDevice = userDeviceString.split(",");

		Gson gson = new Gson();

		Map<String, Object> map = new HashMap<String, Object>();

		for (String userDeviceInstance : userDevice) {
			String device = userDeviceInstance.split(":")[0];
			Integer timeSpentOnDeivce = Integer.parseInt(userDeviceInstance.split(":")[1]);
			float distribution = 0;

			if (totalTimeSpent == 0) {
				distribution = 0;
			} else {
				distribution = ((float) timeSpentOnDeivce / totalTimeSpent) * 100;
			}

			map.put(device, String.format("%.2f", distribution) + "%");

		}

		String json = gson.toJson(map);

		return new Tuple3<String, Long, Long>(json, totalTimeSpent, buffer.getLong(2));
	}

	/**
	 * This function is called whenever key changes
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, null);
		buffer.update(1, 0l);
		buffer.update(2, 0l);
	}

	/**
	 * Merge two partial aggregates
	 */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		if (buffer1.getString(0) == null) {
			buffer1.update(0, buffer2.getString(0));
		} else
			buffer1.update(0, buffer1.getString(0) + "," + buffer2.getString(0));

		buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
		buffer1.update(2, buffer1.getLong(2) + buffer2.getLong(2));

	}

	/**
	 * Iterate over each entry of a group
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		String bufferDevice = buffer.getString(0);
		String inputDevice = input.getString(0);
		Long inputTimeSpent = input.getLong(1);
		Long inputvideoStart = input.getLong(2);

		Long bufferTimeSpent = buffer.getLong(1);
		Long bufferVideoStart = buffer.getLong(2);

		Long totalTimeSpent = inputTimeSpent + bufferTimeSpent;
		Long totalVideoStart = inputvideoStart + bufferVideoStart;

		if (bufferDevice == null)
			buffer.update(0, inputDevice + ":" + inputTimeSpent);
		else
			buffer.update(0, bufferDevice + "," + inputDevice + ":" + inputTimeSpent);

		buffer.update(1, totalTimeSpent);

		buffer.update(2, totalVideoStart);

	}

}
