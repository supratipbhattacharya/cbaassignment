/**
 * 
 */
package com.tech.assignment.dataanalytics.serviceimpl;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Component;

import com.tech.assignment.dataanalytics.mapper.CustomerMapper;
import com.tech.assignment.dataanalytics.mapper.TransactionsMapper;
import com.tech.assignment.dataanalytics.models.Customer;
import com.tech.assignment.dataanalytics.models.Transactions;
import com.tech.assignment.dataanalytics.util.CommonConstants;

/**
 * @author supratip
 *
 */

@Component
public class DataAnalytics {

	public void findDataSet() {
		
		try {
			//Create a Spark session
			SparkSession sparkSession = SparkSession
					.builder()
					.appName("CSV to Output")
					.master("local")
					.getOrCreate();
			
			Dataset<Row> customerDF = readCustomerData(sparkSession);
			Dataset<Row> transactionDF = readTransactionsData(sparkSession);
			
			//Mask customer post code
			maskCustomerPostCode(customerDF);
			
			//Apply 5 years of age bucketing
			ageBucketing(customerDF);
			
			//Filter customer younger than 20
			filterCustomerBasedOnAge(customerDF);
			
			//Join customer and transactions
			Dataset<Row> joinedCustomerDF = customerDF.join(transactionDF, customerDF.col("personId").equalTo(transactionDF.col("customerId")));
			
			//Update Loyal_Customer flag
			updateLoyalCustomer(joinedCustomerDF);
			
			//Filter Summary of spending by days of week
			findDayWiseTransaction(joinedCustomerDF);
			
			//Filter out customers that did not transact at all
			Dataset<Row> customerNotTransactDF = customerDF.join(transactionDF, customerDF.col("personId").notEqual(transactionDF.col("customerId")))
					.select(customerDF.col("personId"),
							customerDF.col("postcode"),
							customerDF.col("state"),
							customerDF.col("gender"),
							customerDF.col("age"),
							customerDF.col("accountType"),
							customerDF.col("loyalCustomer")
							);
			customerNotTransactDF.show(114);
			
			//Filter out transactions of customers that do not present in the customer dataset
			Dataset<Row> transactionNotPresentDF = transactionDF.join(customerDF, customerDF.col("personId").equalTo(transactionDF.col("customerId")), "left_anti");
			transactionNotPresentDF.show(112);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param joinedCustomerDF
	 */
	private void findDayWiseTransaction(Dataset<Row> joinedCustomerDF) {
		
		joinedCustomerDF = joinedCustomerDF.withColumn("date", to_date(joinedCustomerDF.col("date"), "d/MM/yyyy"));
		joinedCustomerDF.show(52);
		joinedCustomerDF.printSchema();
		
		//Dataset<Row> joinedCustomerBasedOnDays = joinedCustomerDF.withColumn("days_of_week", dayofweek(joinedCustomerDF.col("date")));
		
		Dataset<Row> joinedCustomerBasedOnDays = joinedCustomerDF.withColumn("days_of_week", date_format(joinedCustomerDF.col("date"), "E"));
		joinedCustomerBasedOnDays.select(joinedCustomerBasedOnDays.col("date"), joinedCustomerBasedOnDays.col("total"), joinedCustomerBasedOnDays.col("days_of_week")).show(52);
		joinedCustomerBasedOnDays = joinedCustomerBasedOnDays.filter(joinedCustomerBasedOnDays.col("days_of_week").isNotNull());
		joinedCustomerBasedOnDays.show(52);
		
		Dataset<Row> joinedCustomerBasedOnWedDays = joinedCustomerBasedOnDays.filter(joinedCustomerBasedOnDays.col("days_of_week").equalTo("Wed"));
		joinedCustomerBasedOnWedDays.show(52);
		
		joinedCustomerBasedOnWedDays = joinedCustomerBasedOnDays.withColumn("cap_total", joinedCustomerBasedOnWedDays.col("Total"));
		joinedCustomerBasedOnWedDays.show(52);
		
		joinedCustomerBasedOnWedDays = joinedCustomerBasedOnWedDays.filter(joinedCustomerBasedOnWedDays.col("Total") + ">= 100");
		joinedCustomerBasedOnWedDays = joinedCustomerBasedOnWedDays.withColumn("cap_total", lit(99));
		joinedCustomerBasedOnWedDays.show(52);
		
		joinedCustomerBasedOnDays = joinedCustomerBasedOnDays.groupBy(joinedCustomerBasedOnDays.col("days_of_week")).agg(sum(joinedCustomerBasedOnDays.col("total")));
		
		joinedCustomerBasedOnDays.show(52);
	}

	/**
	 * 
	 * @param joinedDF
	 */
	private void updateLoyalCustomer(Dataset<Row> joinedCustomerDF) {
	
		Dataset<Row> joinedCustomerNonLoyalDF = joinedCustomerDF.filter(joinedCustomerDF.col("Total") + "< 1000");
		joinedCustomerNonLoyalDF.show(52);
		
		joinedCustomerNonLoyalDF = joinedCustomerNonLoyalDF.withColumn("loyalCustomer", lit(false));
		joinedCustomerNonLoyalDF.show(52);
		
		Dataset<Row> joinedCustomerLoyalDF = joinedCustomerDF.filter(joinedCustomerDF.col("Total") + "> 1000");
		joinedCustomerLoyalDF.show(52);
		
		joinedCustomerLoyalDF = joinedCustomerLoyalDF.withColumn("loyalCustomer", lit(true));
		joinedCustomerLoyalDF.show(52);
		
		joinedCustomerDF = joinedCustomerLoyalDF.union(joinedCustomerNonLoyalDF);
		
		joinedCustomerDF.show(114);
	}

	/**
	 * filter out customers younger than 20
	 * @param customerDF
	 */
	private void filterCustomerBasedOnAge(Dataset<Row> customerDF) {
		
		customerDF = customerDF.filter(
				customerDF.col("age") + "< 20");
		customerDF.show(114);
	}

	/**
	 * Age column apply 5 years bucketing
	 * @param customerDF
	 */
	private void ageBucketing(Dataset<Row> customerDF) {
		
		converAgeBucket(customerDF);
	}

	/**
	 * 
	 * @param customerDF
	 */
	private void converAgeBucket(Dataset<Row> customerDF) {

		Dataset<Row> customerFDF = customerDF.filter(customerDF.col("age") + ">=15")
				.filter(customerDF.col("age") + "<= 19");
		customerFDF = customerFDF.withColumn("age_range", concat(lit("["), lit(15), lit("-"), lit(19), lit("]")));
		customerFDF.show(52);

		Dataset<Row> customerSDF = customerDF.filter(customerDF.col("age") + ">=20")
		         .filter(customerDF.col("age") + "<= 24");
		customerSDF = customerSDF.withColumn("age_range", concat(lit("["), lit(20), lit("-"), lit(24), lit("]")));
		customerSDF.show(52);
		
		Dataset<Row> customerTDF = customerDF.filter(customerDF.col("age") + ">=30")
		         .filter(customerDF.col("age") + "<= 34");
		customerTDF = customerTDF.withColumn("age_range", concat(lit("["), lit(30), lit("-"), lit(34), lit("]")));
		customerTDF.show(52);
		
		Dataset<Row> customerFoDF = customerDF.filter(customerDF.col("age") + ">=40")
		         .filter(customerDF.col("age") + "<= 44");
		customerFoDF = customerFoDF.withColumn("age_range", concat(lit("["), lit(40), lit("-"), lit(44), lit("]")));
		customerFoDF.show(52);
		
		Dataset<Row> customerFiDF = customerDF.filter(customerDF.col("age") + ">=50")
		         .filter(customerDF.col("age") + "<= 54");
		customerFiDF = customerFiDF.withColumn("age_range", concat(lit("["), lit(50), lit("-"), lit(54), lit("]")));
		customerFiDF.show(52);
		
		customerDF = customerFDF.union(customerSDF).union(customerTDF).union(customerFoDF).union(customerFiDF);
		customerDF.show(112);
	}
		

	/**
	 * For each combination of state, gender and age mask postcode of the
	 * customer if cell size is greater than 5
	 * @param customerDF
	 */
	private void maskCustomerPostCode(Dataset<Row> customerDF) {
		
		//Cast age column from integer to String
		customerDF = customerDF.withColumn("age", customerDF.col("age").cast(DataTypes.StringType));
		
		//Filter Rows based on the combination of state, gender and age length if greater than 5
		customerDF = customerDF.filter(
				length(concat(customerDF.col("state"), customerDF.col("gender") , customerDF.col("age"))) + "> 5");
		
		//Masked postcode based on the above filtered output
		customerDF = customerDF.withColumn("postcode", lit(CommonConstants.MASKED_POST_CODE));
		customerDF.show(50);
	}

	/**
	 * 
	 * @param sparkSession 
	 * @return
	 */
	public Dataset<Row> readCustomerData(SparkSession sparkSession) {
		
		//Create DataFrame Row from customer csv file 
		Dataset<Row> df = getDataSet(sparkSession, CommonConstants.CUSTOMER_CSV_URL);
		
		//Create DataSet from customer DataFrame using map function and user defined Customer model
		Dataset<Customer> customerDS = df.map(new CustomerMapper(), Encoders.bean(Customer.class));
		customerDS.printSchema();
		
		//Convert Customer DataSet to DataFrame
		Dataset<Row> customerDF = customerDS.toDF();
		customerDF.show(50);
		
		return customerDF;
	}

	
	/**
	 * 
	 * @param sparkSession 
	 * @return
	 */
	public Dataset<Row> readTransactionsData(SparkSession sparkSession) {

		//Create DataFrame Row from transactions csv file 
		Dataset<Row> df = getDataSet(sparkSession, CommonConstants.TRANSACTIONS_CSV_URL);
		
		//Create DataSet from customer DataFrame using map function and map into user defined Transactions model
		Dataset<Transactions> transactionDS = df.map(new TransactionsMapper(), Encoders.bean(Transactions.class));
		
		transactionDS.printSchema();
		
		//Convert Customer DataSet to DataFrame
		Dataset<Row> transactionDF = transactionDS.toDF();
		
		transactionDF.show(50);
		
		return transactionDF;
		
	}
	
	/**
	 * 
	 * @param sparkSession
	 * @param fileUrl
	 * @return
	 */
	private Dataset<Row> getDataSet(SparkSession sparkSession, String fileUrl) {
		Dataset<Row> df = sparkSession.read().format("csv")
		        .option("inferSchema", "true")
		        .option("header", true)
		        .option("sep", ",")
		        .load(fileUrl);
		
		return df;
	}
}
