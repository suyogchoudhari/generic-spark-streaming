package com.sparktest.job;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.JavaDeserializationStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sparktest.report.StreamingReport;

public class StreamingJob extends AbstractJob{
	private static Logger logger = Logger.getLogger(StreamingJob.class);
	
	/*
	 * sample input
	 * {"country":"US","url":"www.abc.com","jwt":"1234","pageloadtime":10}
	 */
	public static void main(String[] args) {
		try{
			loadConifg();
		} catch (IOException e){
			logger.error(e.getMessage());
		}
		
		//create spark context
		String jobName = "Streaming Job";
		logger.info(jobName);
		SparkConf conf = new SparkConf().setAppName(jobName);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		String topic = (String)config.get("topic");
		if(topic != null){
			logger.info("topic::"+topic);
			// read topic from Kafka
			int numThreads = 1;
			Map<String, Integer> topicMap = new HashMap<>();
		    String[] topics = topic;
		    for (String topic: topics) {
		      topicMap.put(topic, numThreads);
		    }
		    
		    /*String zookeepr = "zk";
		    String consumerGroup = "cg";
		    JavaPairReceiverInputDStream<String, String> kafkaMessages = KafkaUtils.createStream(jssc, zookeepr, consumerGroup, topicMap);
			
		    JavaDStream<String> messages = kafkaMessages.map(new Function<Tuple2<String, String>, String>() {
		        @Override
		        public String call(Tuple2<String, String> tuple2) {
		          return tuple2._2();
		        }
		    });*/
		    
		    //use following code to read from nc -lk 7777
			//JavaDStream<String> messages = jssc.socketTextStream("localhost", 7777);
		
			startProcessingMessages(messages);
		
			jssc.start();
			try {
				jssc.awaitTermination();
			} catch (InterruptedException e){
				logger.error(e.getMessage());
			}
		}
	}
	
	public static void startProcessingMessages(JavaDStream<String> messages) {
		logger.info("Starting message processing");
		
		List<Map<String,String>> reports = (List<Map<String,String>>)config.get("reports");
	    
	    for(Map<String,String> report : reports) {
	    	logger.info("-----------");
	    	logger.info(report.get("report"));
	    	new StreamingReport().streamReport(messages, report.get("report"), report.get("query"));
	    }
	}
}
