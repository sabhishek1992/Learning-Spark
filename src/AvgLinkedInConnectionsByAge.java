import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AvgLinkedInConnectionsByAge {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext("local[*]","Learning-Spark");
		
		JavaRDD<String> rdd1 = sc.textFile("C:/user/abhishek/sparkinput/friendsdata-201008-180523.csv");
		//row-id, name, age, noOfConnections
		
		 JavaPairRDD<String, Tuple2<Double,Double>> rdd2 = rdd1.mapToPair(x -> {
			String[] arr = x.split("::");
			return new Tuple2<>(arr[2], new Tuple2<>(Double.parseDouble(arr[3]),1.0));
		});
		 
		 JavaPairRDD<String, Tuple2<Double, Double>> rdd3 = rdd2.reduceByKey((x,y) -> new Tuple2<>(x._1+y._1,x._2+y._2));
		 
		 JavaRDD<Tuple2<String,Double>> rdd4 = rdd3.map(x -> new Tuple2<>(x._1,x._2._1/x._2._2));
		 
		 JavaRDD<Tuple2<String, Double>> rdd5 = rdd4.sortBy(x -> x._2,false,1);
		 
		 rdd5.collect().forEach(s -> System.out.println(s));
	}
	
	
}
