import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class WithColumn {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("learning-spark");
		sparkConf.setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

		Dataset<Row> ordersDf = spark.read().format("csv").option("header",
				true).option("inferSchema", true) .option("path",
						"/user/abhishek/sparkinput/orders-201025-223502.csv").load();

		ordersDf.printSchema();
		
		ordersDf.withColumn("order_date", functions.unix_timestamp(functions.col("order_date").cast(DataTypes.TimestampType)))
		.withColumn("newId", functions.monotonically_increasing_id()).show(false);		
		
	}
}
