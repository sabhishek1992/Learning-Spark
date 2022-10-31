import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		
		//ALL<DEBUG<INFO<WARN<ERROR<FATAL<OFF
		
		//Logs will appear for Level >=ERROR i.e. 
		Logger.getLogger("org").setLevel(Level.ERROR);		
		
		JavaSparkContext sc = new JavaSparkContext("local[*]", "Learing-Spark");
		
		JavaRDD<String> rdd1 = sc.textFile("/user/abhishek/sparkinput/search.txt", 1);
		
		JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		
		JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(x -> new Tuple2<>(x, 1));

		JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey((x,y) -> x+y);
		
		List<Tuple2<String, Integer>> localVar = rdd4.collect();
		
		localVar.forEach(x -> System.out.println(x));
		
		SparkConf sparkConf = new SparkConf().setAppName("Learning-Spark").setMaster("local[*]");
		
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		
		StructType schema = StructType.fromDDL("word String");
		
		
		
		
		
	}
}
