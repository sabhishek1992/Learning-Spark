import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Top10ShopaholicCustomer {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);		
		
		JavaSparkContext sc = new JavaSparkContext("local[*]", "Learing-Spark");
		
		JavaRDD<String> rdd1 = sc.textFile("/user/abhishek/sparkinput/customerorders-201008-180523.csv",1);
		//44,8602,37.19
		//customer-id,product-id,amount-spent
		
		JavaPairRDD<String, Double> rdd2 = rdd1.mapToPair(x -> {
			String[] arr = x.split(",");
			return new Tuple2<>(arr[0], Double.parseDouble(arr[1]));
		});
		
		JavaPairRDD<String, Double> rdd3 = rdd2.reduceByKey((x,y) -> x+y);
		JavaPairRDD<String, Double> rdd4 = rdd3.sortByKey(false);//descending high-to-low
		
		rdd4.take(10).forEach(s -> System.out.println(s));
	}
}
