///**
// * 
// */
//package com.tech.assignment.dataanalytics.datafactory;
//
//import java.util.List;
//
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.SparkSession;
//import org.springframework.stereotype.Component;
//
//import com.tech.assignment.dataanalytics.models.Customer;
//import com.tech.assignment.dataanalytics.models.Transactions;
//
///**
// * @author supra
// *
// */
//
////@Component
//public class DataFactory {
//
//	private static final String CUSTOMER_CSV_URL = "src/main/resources/customer.csv";
//	private static final String TRANSACTIONS_CSV_URL = "src/main/resources/transactions.csv";
//	private static volatile DataFactory me;
//
//	public DataFactory() {
//
//	}
//
//	/**
//	 * 
//	 * @return
//	 */
//	public static DataFactory getInstance() {
//		if (me == null)
//			synchronized (DataFactory.class) {
//				me = new DataFactory();
//			}
//		return me;
//	}
//
//	/**
//	 * 
//	 * @return
//	 */
//	public List<Customer> readCustomerData() {
//
//		SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
//		JavaRDD<Customer> customerRDD = spark.read()
//
//				.textFile(CUSTOMER_CSV_URL)
//				.javaRDD()
//				.map(line -> {
//					String[] parts = line.split(",");
//
//					Customer customer = new Customer();
//
//					customer.setPersonId(parts[0]);
//					customer.setPostCode(parts[1]);
//					customer.setAccountType(line);
//					customer.setAge(line);
//					customer.setGender(line);
//					customer.setLoyalCustomer(line);
//					customer.setState(line);
//
//					return customer;
//
//				});
//
//		return customerRDD.collect();
//	}
//	
//	/**
//	 * 
//	 * @return
//	 */
//	public List<Transactions> readTransactionsData() {
//
//		SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
//		JavaRDD<Transactions> customerRDD = spark.read()
//
//				.textFile(TRANSACTIONS_CSV_URL)
//				.javaRDD()
//				.map(line -> {
//					String[] parts = line.split(",");
//
//					Transactions transactions = new Transactions();
//
//					transactions.setCustomerId(parts[0]);
//
//					return transactions;
//
//				});
//
//		return customerRDD.collect();
//	}
//
//}
