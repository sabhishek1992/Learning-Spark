import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class HashAndSortAggregates {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf sparkConf = new SparkConf().setAppName("Learning-Spark").setMaster("local[*]");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
		
		StructType schemaType = new StructType(new StructField[] {
				new StructField("order_id", DataTypes.StringType,false, Metadata.empty()),
				new StructField("order_date", DataTypes.StringType,false, Metadata.empty()),
				new StructField("customer_id", DataTypes.StringType,false, Metadata.empty()),
				new StructField("order_status", DataTypes.StringType,false, Metadata.empty())
		});
		
		Dataset<Row> ordersDf = spark.read().format("csv").schema(schemaType).option("header",
				true).option("path",
						"/user/abhishek/sparkinput/orders-201025-223502.csv").load();
		ordersDf.createOrReplaceTempView("orders");
		
		
		//order_id,order_date,customer_id,order_status
		
		Dataset<Row> sortAggDS = spark.sql("select customer_id, date_format(order_date,'MMMM') as month, first(date_format(order_date,'M')) as month_num,  count(*) as no_of_orders from orders group by customer_id, month order by cast(month_num as int)");
		
		sortAggDS.explain();
		
		Dataset<Row> hashAggDS = spark.sql("select customer_id, date_format(order_date,'MMMM') as month, first(cast(date_format(order_date,'M') as int)) as month_num, count(*) as no_of_orders from orders group by customer_id, month order by month_num");
		
		hashAggDS.explain();
		
		new Scanner(System.in).next();
	}
}
