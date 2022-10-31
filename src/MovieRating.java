import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MovieRating {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext("local[*]","Learning-Spark");
		
		JavaRDD<String> rdd1 = sc.textFile("C:/user/abhishek/sparkinput/moviedata-201008-180523.data");
		//user-id,movie-id,rating,timestamp
		
		//how many times movie was rated 5,4,3,2,1 stars
		
		JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(x -> {
			String[] arr = x.split("\t");
			return new Tuple2<>(arr[2], 1);
		});
		
		JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey((x,y) -> x+y);
		
		JavaPairRDD<String, Integer> rdd4 = rdd3.sortByKey(false);
		
		rdd4.collect().forEach(s -> System.out.println(s));
		
		System.out.println(rdd2.countByValue());
	}
}
