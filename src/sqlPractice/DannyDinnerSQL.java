package sqlPractice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DannyDinnerSQL {

public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("DannyDinnerSQL");
		sparkConf.setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
		
		Dataset<Row> salesDS = spark.read().format("csv").option("header", true)
		.option("path", "C:\\_Abhishek\\STUDY\\TrendyTech\\SQL Interview Series\\sales.csv")
		.load();
		
		salesDS.createOrReplaceTempView("salesDS");
		
//		spark.sql("create table if not exists sales(customer_id STRING,order_date DATE,product_id INT)");
		
//		spark.sql("insert into sales select customer_id,order_date,product_id from salesDS");
		
//		spark.sql("select * from sales").show(false);
		
		Dataset<Row> menuDS = spark.read().format("csv").option("header", true)
				.option("path", "C:\\_Abhishek\\STUDY\\TrendyTech\\SQL Interview Series\\menu.csv")
				.load();
		
		menuDS.createOrReplaceTempView("menuDS");
		
//		spark.sql("create table if not exists menu(product_id INT,product_name STRING,price DOUBLE) ");
		
//		spark.sql("insert into menu select * from menuDS");
		
//		spark.sql("select * from menu").show(false);
		
		Dataset<Row> membersDS = spark.read().format("csv").option("header", true)
				.option("path", "C:\\_Abhishek\\STUDY\\TrendyTech\\SQL Interview Series\\members.csv")
				.load();
		
//		membersDS.createOrReplaceTempView("membersDS");
//		
//		spark.sql("create table if not exists members(customer_id STRING,join_date DATE)");
//		
//		spark.sql("insert into members select * from membersDS");
//		
//		spark.sql("select * from members").show(false);
		
		
//		  System.out.println("total amt. each customer spent");
		  
		  spark.sql(" select s.customer_id,sum(m.price) as total_amt " +
		  " from sales s join menu m " +
		  " on s.product_id=m.product_id group by s.customer_id");
		  
//		  System.out.println("no. of days each customer visited");
		  
		  spark.sql("select s.customer_id,count(distinct s.order_date) " +
		  "visits_per_day from sales s group by s.customer_id");
		 
		
//		System.out.println("first menu item purchased by customer");
		
		
		  spark.sql("select s.customer_id,collect_list(m.product_name) " +
		  "as product_name," + " order_date  from sales s join menu m " +
		  " on s.product_id = m.product_id group by s.customer_id,order_date " +
		  " having order_date = (select min(s1.order_date) from sales s1 where s1.customer_id=s.customer_id)"
		  );
		 
		
		
		//first date of customer visit
		
		spark.sql("with first_visit as (select min(s.order_date) as order_date,s.customer_id from sales s group by s.customer_id), "
		+
		" first_order_for_customer as (select s1.customer_id,s1.product_id from sales s1 join first_visit f on s1.customer_id=f.customer_id and s1.order_date=f.order_date) "
		+
		" select distinct f.customer_id,m.product_name from first_order_for_customer f join menu m on f.product_id=m.product_id");
		
		//What is the most purchased item on
//		the menu and how many times was it
//		purchased by all customers?
		
		
		//most purchased item
	
		spark.sql("select product_name,count(s.product_id) as count from sales s join menu m on s.product_id=m.product_id group by m.product_name order by count desc limit 1");
		
		//most popular item for each customer

		spark.sql("with customer_order_count as (select customer_id,product_id,count(product_id) count from sales group by customer_id,product_id), "
		+
		"customer_product_rank as (select customer_id,product_id,count, dense_rank() over(partition by customer_id order by count desc) rank from customer_order_count) "
		+
		"select customer_id,product_name from customer_product_rank c join menu m on c.product_id = m.product_id where rank=1"			
		);
		
		 spark.sql("with product_rank as (select s.customer_id,m.product_name,count(m.product_name) as count,"
		 +"dense_rank() over (partition by s.customer_id order by count(m.product_name) desc) as rank "
		 +"from sales s join menu m on s.product_id=m.product_id group by s.customer_id,m.product_name)"
		 + "select * from product_rank where rank=1");
		
		//Which item was purchased first by the
//		 customer after they became a
//		 member?
		
		 spark.sql("with product_rank as (select s.customer_id,s.product_id,s.order_date,dense_rank() over (partition by s.customer_id order by s.order_date asc) as rank "
		 		+ "from sales s join members m on s.customer_id=m.customer_id where s.order_date > m.join_date) "
				+ "select p.customer_id,m.product_name from product_rank p join menu m on p.product_id=m.product_id where rank=1"
				 
				 );
		//Which item was purchased just before
//		 the customer became a member?
		
		 spark.sql("with product_rank as (select s.customer_id,s.product_id,s.order_date,dense_rank() over (partition by s.customer_id order by s.order_date desc) as rank "
			 		+ "from sales s join members m on s.customer_id=m.customer_id where s.order_date < m.join_date) "
					+ "select p.customer_id,m.product_name,p.order_date from product_rank p join menu m on p.product_id=m.product_id where rank=1"
					 
					 );
		 
//		 What is the total items and amount
//		 spent for each member before they
//		 became a member?
		 
		 spark.sql("with order_details as (select s.customer_id,me.product_name,me.price from sales s join members m on s.customer_id=m.customer_id  "
		 		+ "join menu me on s.product_id=me.product_id where s.order_date < m.join_date) "
				 
				+ "select customer_id,count(product_name) as total_items, sum(price) as amt_spent from order_details group by customer_id" 
				 );
		 
		 //If each $1 spent equates to 10 points
//		 and sev has a 2x points multiplier —
//		 how many points would each
//		 customer have?
		 
		 spark.sql("with amt_spent_by_customer as (select s.customer_id,m.product_name,case when product_name='sev' then m.price*20 else m.price*10 end as points "
		 		+ " from sales s join menu m on s.product_id=m.product_id )"
				+ " select customer_id,sum(points) as total_points from amt_spent_by_customer group by customer_id " 
				 );
		
		//In the first week after a customer joins
//		 the program (including their join date)
//		 they earn 2x points on all items, not
//		 just sev — how many points do
//		 customer A and B have at the end of
//		 January? i.e. starting from joining date till 7 days they earn 2x points
		
		 
		 spark.sql("with order_details as (select s.customer_id,me.product_name,me.price,datediff(s.order_date,m.join_date) as first_week from sales s join menu me on s.product_id=me.product_id "
		 		+ " join members m on s.customer_id=m.customer_id where extract(month from order_date)=1) , "
				+ " points_by_customers as (select customer_id,product_name,"
				+ " case when first_week between 0 and 7 then price*20 "
				+ " 	 when product_name='sev' then price*20 "
				+ " 	 else price*10 end as points "
				+ " from order_details )"
				+ " select customer_id,sum(points) as total_points from points_by_customers group by customer_id"
				 ).show();
		 
}
		
}
