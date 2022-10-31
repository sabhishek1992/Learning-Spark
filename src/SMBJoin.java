import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SMBJoin {

	public static void main1(String[] args) throws AnalysisException {

		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("learning-spark");
		sparkConf.setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

		spark.sql("select * from mydb.customer_bucketed c join mydb.orders_bucketed o "
				+ "on c.customer_id = o.customer_id").show(false);
		
		spark.catalog().listColumns("mydb.customer_bucketed").show();
		spark.catalog().listColumns("mydb.orders_bucketed").show();
		
		new Scanner(System.in).next();
		
	}
	
	public static void main(String[] args) throws AnalysisException {

		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("learning-spark");
		sparkConf.setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

		spark.sql("create database if not exists mydb");
		
		Dataset<Row> ordersDf = spark.read().format("csv").option("header",
				true).option("inferSchema", true) .option("path",
						"/user/abhishek/sparkinput/orders-201025-223502.csv").load();
		
		
//		ordersDf.write().format("csv").mode(SaveMode.Overwrite)
//				 .bucketBy(4, "customer_id").sortBy("customer_id")
//				 .saveAsTable("mydb.orders_bucketed");
//		
		ordersDf.createOrReplaceTempView("orders");
//		ordersDf.printSchema();

		Dataset<Row> customersDf = spark.read().format("csv").option("header",
				true).option("inferSchema", true) .option("path",
						"/user/abhishek/sparkinput/customers-201025-223502.csv").load();
		
//		customersDf.write().format("csv").mode(SaveMode.Overwrite)
//		.bucketBy(4, "customer_id").sortBy("customer_id")
//		.saveAsTable("mydb.customer_bucketed");
		
		customersDf.createOrReplaceTempView("customers");
//		customersDf.printSchema();
		
		spark.sql("select o.*,c.customer_fname,c.customer_lname from orders o join customers c on o.customer_id=c.customer_id")
		.show(false);

		spark.catalog().listTables("mydb").show();
		spark.catalog().listColumns("mydb.customer_bucketed").show();
		spark.catalog().listColumns("mydb.orders_bucketed").show();
		
		new Scanner(System.in).next();
	}
}
