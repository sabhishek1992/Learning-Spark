import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class BoringWordRemovalApp {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext("local[*]","Learning-Spark");
		
		JavaRDD<String> campaignRdd = sc.textFile("C:/user/abhishek/sparkinput/bigdatacampaigndata-201014-183159.csv");
		JavaRDD<String> boringWordsRdd = sc.textFile("/user/abhishek/sparkinput/boringwords-201014-183159.txt");
		
		Set<String> boringWords = new HashSet<>(boringWordsRdd.collect());
		Broadcast<Set<String>> broadcastVar = sc.broadcast(boringWords);
		
		JavaPairRDD<Double, String> rdd2 = campaignRdd.mapToPair(x -> {
			String[] arr = x.split(",");
			return new Tuple2<>(Double.parseDouble(arr[10]),arr[0]);
		});
		
		JavaPairRDD<Double, String> rdd3 = rdd2.flatMapValues(s -> Arrays.asList(s.split(" ")));
		
		 JavaPairRDD<String, Double> rdd4 = rdd3.filter(x -> {
			 return !broadcastVar.value().contains(x._2);
		 }).mapToPair(x -> new Tuple2<>(x._2,x._1));
		 
		 JavaPairRDD<String, Double> rdd5 = rdd4.reduceByKey((x,y) -> x+y);
		 
		 JavaRDD<Tuple2<String, Double>> rdd6 = rdd5.map(x -> new Tuple2<>(x._1,x._2)).sortBy(x -> x._2, false, 1);
		 
		 rdd6.foreach(x -> System.out.println(x));
	}
}
