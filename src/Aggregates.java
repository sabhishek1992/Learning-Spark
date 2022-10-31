import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Aggregates {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("learning-spark");
		sparkConf.setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
		
		
		Dataset<Row> ordersDf = spark.read().format("csv").option("header",
		  true).option("inferSchema", true) .option("path",
		  "/user/abhishek/sparkinput/orders_small_data.csv").load();
		ordersDf.createOrReplaceTempView("orders");
		
		ordersDf.printSchema();
		
		Dataset<Row> customersDf = spark.read().format("csv").option("header",
				  true).option("inferSchema", true) .option("path",
				  "/user/abhishek/sparkinput/customers_small_data.csv").load();
		customersDf.createOrReplaceTempView("customers");
		
		
		//Simple Join - Inner
		spark.sql("select o.*,c.customer_fname,c.customer_lname from orders o right join customers c on o.customer_id=c.customer_id")
		.show(false);
		
		
		/*
		 * //Simple Aggregate
		 * spark.sql("select count(*) as row_count, sum(Quantity) as total_quantity, " +
		 * "avg(UnitPrice) as avg_price, count(distinct(InvoiceNo)) as count_distinct" +
		 * " from orders").show(false);
		 * 
		 * //Grouping Aggregate spark.sql(" select Country, sum(Quantity * UnitPrice) "
		 * + " over(partition by Country order by InvoiceNo " +
		 * " rows between unbounded preceding and current row) " +
		 * " as RunningTotal from orders") .show(false);
		 */
		
		new Scanner(System.in).next();
	}
}
