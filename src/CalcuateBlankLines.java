import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CalcuateBlankLines {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext("local[*]","Learning-spark");
		
		@SuppressWarnings("deprecation")
		Accumulator<Integer> myAccumulator = sc.intAccumulator(0, "blank lines accumulator");
		
		JavaRDD<String> rdd1 = sc.textFile("C:/user/abhishek/sparkinput/samplefile-201014-183159.txt");
		rdd1.foreach(x -> {
			if(x.equals("")) { 
				myAccumulator.add(1);
			}
		}
				);
		
		System.out.println(myAccumulator);
	}
}
