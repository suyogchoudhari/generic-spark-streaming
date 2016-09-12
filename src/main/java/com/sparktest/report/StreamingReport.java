package com.sparktest.report;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;

public class StreamingReport {

	public void streamReport(JavaDStream<String> streamRdd, String report, String query){
		streamRdd.foreachRDD(new VoidFunction<JavaRDD<String>>(){

			@Override
			public void call(JavaRDD<String> javaRdd) throws Exception {
				streamReport(javaRdd, report, query);
			}
		});
	}
	
	public void streamReport(JavaRDD<String> javaRdd, String report, String query){
		if(javaRdd.count()>0){
			SparkSession session = SparkSession.builder().config(javaRdd.context().getConf()).getOrCreate();
			Dataset<Row> dataset = session.read().json(javaRdd);
			dataset.createOrReplaceTempView("dataset");
			Dataset<Row> result = session.sql(query);
			//result.toJSON().collectAsList().
			//publish result to kafka
			result.show();
		}
	}
}
