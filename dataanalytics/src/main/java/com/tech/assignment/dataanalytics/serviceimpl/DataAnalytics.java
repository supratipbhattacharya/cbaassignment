/**
 * 
 */
package com.tech.assignment.dataanalytics.serviceimpl;

import java.util.List;

import javax.xml.validation.SchemaFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.tech.assignment.dataanalytics.mapper.CustomerMapper;
import com.tech.assignment.dataanalytics.mapper.TransactionsMapper;
import com.tech.assignment.dataanalytics.models.Customer;
import com.tech.assignment.dataanalytics.models.Transactions;
import com.tech.assignment.dataanalytics.util.CommonConstants;
import static org.apache.spark.sql.functions.*;

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
			
			maskCustomerPostCode(customerDF);
			
			ageBucketing(customerDF);
			
			filterCustomerBasedOnAge(customerDF);
			
			Dataset<Row> joinedCustomerDF = customerDF.join(transactionDF, customerDF.col("personId").equalTo(transactionDF.col("customerId")));
//			.select(customerDF.col("personId"),
//					customerDF.col("postcode"),
//					customerDF.col("state"),
//					customerDF.col("gender"),
//					customerDF.col("age"),
//					customerDF.col("accountType"),
//					customerDF.col("loyalCustomer")
//					);
			joinedCustomerDF.show(52);
			
			findDayWiseTransaction(joinedCustomerDF);
			
			updateLoyalCustomer(joinedCustomerDF);
			
			//Filter out customers that did not transact at all
			Dataset<Row> customerNotTransactDF = customerDF.join(transactionDF, customerDF.col("personId").notEqual(transactionDF.col("customerId")));
			customerNotTransactDF.show(52);
			
			//filter out transactions of customers that do not present in the customer dataset
			Dataset<Row> customerNotPresentDF = transactionDF.join(customerDF, transactionDF.col("customerId").notEqual(customerDF.col("personId")));
			customerNotPresentDF.show(52);
			
			
			
		} catch (Exception e) {
			System.out.println();
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param joinedCustomerDF
	 */
	private void findDayWiseTransaction(Dataset<Row> joinedCustomerDF) {
		
		//joinedCustomerDF = joinedCustomerDF.withColumn(null, null);
		//Dataset<Row> joinedCustomerDFMonday = joinedCustomerDF.filter((joinedCustomerDF.col("date.date")) + "= 1");
		
		joinedCustomerDF = joinedCustomerDF.withColumn("date", to_date(joinedCustomerDF.col("date"), "d/MM/yyyy"));
		joinedCustomerDF.show(52);
		joinedCustomerDF.printSchema();
		
		//Dataset<Row> joinedCustomerBasedOnDays = joinedCustomerDF.withColumn("days_of_week", dayofweek(joinedCustomerDF.col("date")));
		
		Dataset<Row> joinedCustomerBasedOnDays = joinedCustomerDF.withColumn("days_of_week", date_format(joinedCustomerDF.col("date"), "E"));
		joinedCustomerBasedOnDays.select(joinedCustomerBasedOnDays.col("date"), joinedCustomerBasedOnDays.col("total"), joinedCustomerBasedOnDays.col("days_of_week")).show(52);
		joinedCustomerBasedOnDays = joinedCustomerBasedOnDays.filter(joinedCustomerBasedOnDays.col("days_of_week").isNotNull());
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
		
		joinedCustomerDF.show(52);
	}

	/**
	 * filter out customers younger than 20
	 * @param customerDF
	 */
	private void filterCustomerBasedOnAge(Dataset<Row> customerDF) {
		
		customerDF = customerDF.filter(
				customerDF.col("age") + "> 20");
		
		customerDF.show(52);
	}

	/**
	 * 
	 * @param customerDF
	 */
	private void ageBucketing(Dataset<Row> customerDF) {
		// TODO Auto-generated method stub
		
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
		customerDF.show(52);
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
		
		System.out.println("*****Customer ingested in a dataset: *****");
		//customerDS.printSchema();
		
		//Convert Customer DataSet to DataFrame
		Dataset<Row> customerDF = customerDS.toDF();
		
		//customerDF.show(50);
		
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
		
		System.out.println("*****Transactions ingested in a dataset: *****");
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
		        .option("inferSchema", "true") // Make sure to use string version of true
		        .option("header", true)
		        .option("sep", ",")
		        .load(fileUrl);
		
		return df;
	}
}
