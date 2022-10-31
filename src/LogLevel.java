import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LogLevel {

	public static void main(String[] args) {
	
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext("local[*]","Learning-spark");
	
		/*
		 * List<String> myList = Arrays.asList("WARN: Tuesday 4 September 0405",
		 * "ERROR: Tuesday 4 September 0408", "ERROR: Tuesday 4 September 0408",
		 * "WARN: Tuesday 4 September 0408", "ERROR: Tuesday 4 September 0408");
		 * 
		 * JavaRDD<String> myRdd = sc.parallelize(myList);
		 */
		
		JavaRDD<String> bigLogRdd = sc.textFile("C:/user/abhishek/sparkinput/bigLog.txt");
			  
		JavaPairRDD<String, Integer> logLevelRdd = bigLogRdd.mapToPair(x -> {
			String[] arr = x.split(":");
			return new Tuple2<>(arr[0],1);
		});
		
		/*
		 * List<Tuple2<String, Iterable<Integer>>> localVar =
		 * logLevelRdd.groupByKey().collect(); for (Tuple2<String, Iterable<Integer>> t
		 * : localVar) { List<Integer> list = new ArrayList<>();
		 * t._2().forEach(list::add); System.out.println(t._1 + ", " + list.size()); }
		 */
		
		logLevelRdd.reduceByKey((x,y) -> x+y).count();
		logLevelRdd.reduceByKey((x,y) -> x+y).collect().forEach(x -> System.out.println(x));
		
		
		new Scanner(System.in).next();
	}
}
