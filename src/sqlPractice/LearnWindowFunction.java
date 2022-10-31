package sqlPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LearnWindowFunction {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Learning-Window-function");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> emp_dept_tblDS = spark.read().format("csv").option("header", true)
				.option("path", "C:\\_Abhishek\\STUDY\\TrendyTech\\SQL Interview Series\\test_window.csv")
				.load();
		
		emp_dept_tblDS.createOrReplaceTempView("emp_dept_tbl");
		
//		emp_dept_tblDS.show(false);
		
		//row_number : assigns each row with unique value
		
		spark.sql("SELECT department, salary" + 
				", ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num " + 
				"FROM emp_dept_tbl");
		
		//rank : assigns rank to rows based on column in OVER. Equal values are assigned same rank. Next rank skipped.
		
		spark.sql(" SELECT department, salary"+
					", RANK() OVER(PARTITION BY department ORDER BY salary DESC) AS rnk "+ 
					" FROM emp_dept_tbl");
		
		//dense_rank : assigns rank to rows based on column in OVER. Equal values are assigned same rank. No rank skipped.
		
		
		spark.sql("SELECT department, salary"+
				", DENSE_RANK() OVER(PARTITION BY department ORDER BY salary DESC) AS dns_rnk "+ 
				" FROM emp_dept_tbl");
		
		//cum_dist : It computes the relative position of a column value in a group. 
		//cum_dist(salary) : no. of rows with value <= salary/total no. of rows (ASC)
		
		spark.sql("SELECT department, salary " + 
				", CUME_DIST() OVER (PARTITION BY department  ORDER BY salary DESC) AS cum_dist " + 
				"FROM emp_dept_tbl");
		
		//percent_rank : (rank-1)/remaining no. of rows.
		
		spark.sql("SELECT department, salary, "+
					" RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk, "+
				    " PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS perc_rnk FROM emp_dept_tbl");
		
		//n-tile : t divides the number of rows in a partition into a specific number of ranked groups (bucket) as equally as possible.
		//It returns a bucket member associated with it.
		//no. of rows % ntiles = no. of buckets which will be assigned 1 more value than other buckets
		//5%3=2 ---> 2 buckets (1,2) will be assigned 1 each extra value then bucket(3)
		
		
		spark.sql("SELECT department, salary "+
				 ", NTILE(3) OVER (PARTITION BY department ORDER BY salary DESC) AS ntile "+ 
				 " FROM emp_dept_tbl").show(false);
		
		
		
	}
}
