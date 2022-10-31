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

public class ReadLoadCsvUsingSparkSession {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf sparkConf = new SparkConf().setAppName("Learning-Spark").setMaster("local[*]");
		
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		sparkSession.sparkContext().setCheckpointDir("/user/abhishek/sparkinput/checkpointDir/");
		
		StructField[] structFields = new StructField[3];
		structFields[0]=new StructField("order_id", DataTypes.IntegerType, false,Metadata.empty());
		structFields[1]=new StructField("customer_id", DataTypes.IntegerType, false,Metadata.empty());
		structFields[2]=new StructField("price", DataTypes.DoubleType, false,Metadata.empty());
		
		StructType ordersSchema = StructType.fromDDL("order_id Integer,customer_id Integer,price Double");
		
		Dataset<Row> customerOrdersDS = sparkSession.read().format("csv")
				.option("header", true)
				.schema(ordersSchema)
				.option("path", "C:/user/abhishek/sparkinput/customerorders-201008-180523.csv").load();
		
		
		customerOrdersDS.explain(true);
		
		customerOrdersDS.show();
		
		System.out.println(customerOrdersDS.toJavaRDD().toDebugString());
		
		System.out.println("defaultMinPartitions: " + sparkSession.sparkContext().defaultMinPartitions());
		System.out.println("defaultParallelism: " + sparkSession.sparkContext().defaultParallelism());
		
//		new Scanner(System.in).next();
	}
}
