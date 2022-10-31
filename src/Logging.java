import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class Logging {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("learning-spark");
		sparkConf.setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
	
		Dataset<Row> bigLog = spark.read().format("csv").option("header", true).option("inferSchema", true)
		.option("path","/user/abhishek/sparkinput/biglog-201105-152517.txt").load();
		bigLog.createOrReplaceTempView("bigLog");
		
		Dataset<Row> logLevelByMonth = spark.sql("select level,date_format(datetime,'MMMM') as month, "
				+ "cast(date_format(datetime,'M') as int) as monthNum from bigLog");
		logLevelByMonth.createOrReplaceTempView("logLevelByMonth");
		
		Dataset<Row> logLevelGrped = spark.sql("select level,month,first(monthNum) as monthNum,count(*) from logLevelByMonth "
				+ "group by level,month order by monthNum,level").drop("monthNum");
		
		List<Object> months = Arrays.asList("January","February","March","April","May","June","July","August","September","October","November","December");
		
		logLevelByMonth.groupBy("level").pivot("month", months).count().show(false);
		
	}
}

