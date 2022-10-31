package sqlPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQLInterviewQuestions {

	public static void main(String[] args) {
Logger.getLogger("org").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Learning-Window-function");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> emp_dept_tblDS = spark.read().format("csv").option("header", true)
				.option("path", "C:\\_Abhishek\\STUDY\\TrendyTech\\SQL Interview Series\\test_window.csv")
				.load();
		
		emp_dept_tblDS.createOrReplaceTempView("emp_dept_tbl");
		
		//5th highest salary
		//id,first_name,last_name,designation,department,salary
		/*
		 * spark.sql("select * from emp_dept_tbl emp1 where (5-1) " +
		 * "= (select count(distinct emp2.salary) from emp_dept_tbl emp2 " +
		 * "where emp2.salary > emp1.salary)");
		 */
		
		
		
		spark.sql("with salary_rank (select id,first_name,last_name,designation,department,"
				+ "salary, dense_rank() over(order by salary desc) rank from emp_dept_tbl)"
				+ " select * from salary_rank where rank=5"
				);

		//department-wise 2nd highest salary
		
		spark.sql("with salary_rank (select id,first_name,last_name,designation,department,"
				+ "salary, dense_rank() over(partition by department order by salary desc) rank from emp_dept_tbl)"
				+ " select * from salary_rank where rank=2"
				);
		
		//top-n salaries
		
		spark.sql("with salary_with_rank (select id,first_name,last_name,designation,department,"
				+ " salary, dense_rank() over(order by salary desc) rank"
				+ " from emp_dept_tbl)"
				+ " select * from salary_with_rank where rank<5"
				);
		
		//select duplicate rows from employee table
		
		spark.sql(" with emp_row_num as (select id,first_name,last_name, "
				+ " row_number() over (order by id asc) as rowid from emp_dept_tbl) "
				+ " select * from emp_row_num where rowid not in "
				+ " (select min(rowid) from emp_row_num group by id)"
				);
		
		
		//employees with same salary
		
		spark.sql("select emp1.* from emp_dept_tbl emp1 join emp_dept_tbl emp2 "
				+ " on emp1.salary=emp2.salary and "
				+ "concat(emp1.first_name,emp1.last_name) <> concat(emp2.first_name,emp2.last_name)");
		
		spark.sql("select * from emp_dept_tbl where salary in (select salary from emp_dept_tbl "
				+ "group by salary having count(salary)>=2)");
		
		
		spark.sql("select * from (select e.*, count(*) over(partition by salary order by salary) count from emp_dept_tbl e)"
				+ " where count>1");
		
		
		//employee with salary more than avg
		
		spark.sql("select * from (select *,avg(salary) over (order by salary rows between unbounded preceding and unbounded following) avg_salary "
				+ " from emp_dept_tbl) where salary > avg_salary");
		
		
		//select even no. of rows
		//id,first_name,last_name,designation,department,salary
		//1. assign row_number()
		//2. select * from above tables where serial_no % 2 ==0 ---> even rows
		
		spark.sql("select * from (select *,row_number() over(order by id) row_num from emp_dept_tbl) where row_num%2=0");
		
		Dataset<Row> empDS = spark.read().format("csv").option("header", true).option("inferSchema", true)
		.option("path", "C:\\_Abhishek\\STUDY\\TrendyTech\\SQL Interview Series\\emp.csv").load();
		
		
		empDS.createOrReplaceTempView("emp");
		
		//employees with >1 subordinate
		//empid,department,salary,mgmrid
		//1. group by mgmrid count(*)>1
		
		spark.sql("select * from (select *,count(mgmrid) over(partition by mgmrid) as count from emp ) where count>=2");
		
		//max salary without using function
		
		spark.sql("select * from emp where salary not in (select e1.salary from emp e1 join emp e2 where e1.salary < e2.salary) ");
		
		//count no. of rows without using COUNT() function
		
		spark.sql("select max(row_num) from (select row_number() over(order by empid) row_num from emp)");
		
		//last inserted record
		
		spark.sql("select * from emp where timestamp = (select max(timestamp) from emp)");
		
		//first employee in the company
		
		spark.sql("select * from emp where timestamp = (select min(timestamp) from emp)");
		
		//last n records
		//1. assign row_number
		//2. get max(row_number)
		//3. if row_num >= (max_row_num-n)
		
		spark.sql("with emp_with_row_num as (select *,row_number() over(order by timestamp) row_num from emp),"
				+ "max_row_num as (select max(row_num) row_num from emp_with_row_num) "
				+ "select * from emp_with_row_num where row_num > ((select row_num from max_row_num)-3) "
				
				);
		
		//employees working from last 2 years
		//1. extract month from current timestamp ---> assumption difference in months has to be counted
		//2. constant --> add_months(sysdate,-24)
		
		
		spark.sql("with date_2_years_before as (select add_months(current_date(),-24) date)"
				+ " select * from (select *,to_date(cast(timestamp as timestamp)) join_date from emp) where join_date <=(select date from date_2_years_before) "
				).show();
		
		
		
		
		
		
		
		
	}
}
