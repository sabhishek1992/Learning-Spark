import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class Metastore {

	public static void main(String[] args) throws AnalysisException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("learning-spark");
		sparkConf.setMaster("local[*]");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
		
		/*
		 * Dataset<Row> ordersDf = spark.read().format("csv").option("header",
		 * true).option("inferSchema", true) .option("path",
		 * "/user/abhishek/sparkinput/order_data-201025-223502.csv").load();
		 */
//		ordersDf.write().format("csv").mode(SaveMode.Overwrite).saveAsTable("orders1");
		
//		spark.sql("create database if not exists mydb");
		
		/*
		 * ordersDf.coalesce(1).write().format("csv").mode(SaveMode.Overwrite)
		 * .partitionBy("Country") .bucketBy(4, "InvoiceNo").sortBy("InvoiceNo")
		 * .saveAsTable("mydb.orders3");
		 * 
		 * spark.catalog().listDatabases().show();
		 * spark.catalog().listTables("mydb").show();
		 * spark.catalog().listColumns("mydb.orders3").show();
		 */
//		order_id,order_date,extractFirstNDigits(customer_id),order_status
		spark.udf().register("extractFirstNDigits",new ExtractFirstNDigits(),DataTypes.StringType);
		
		spark.sql("select order_id,order_date,extractFirstNDigits(cast(customer_id as string),2),order_status from mydb.orders2").show();
		
		spark.catalog().listFunctions().show(200,false);
		
		
	}
}