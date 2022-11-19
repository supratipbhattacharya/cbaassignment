/**
 * 
 */
package com.tech.assignment.dataanalytics.mapper;

import java.text.SimpleDateFormat;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.tech.assignment.dataanalytics.models.Transactions;

/**
 * @author supra
 *
 */
public class TransactionsMapper implements MapFunction<Row, Transactions> {

	private static final long serialVersionUID = 1L;

	@Override
	public Transactions call(Row value) throws Exception {
		
		Transactions transaction = new Transactions();
		
		if(value.getAs("cogs") != null) {
			transaction.setCogs(value.getAs("cogs"));
		}
		if(value.getAs("CustomerID") != null) {
			transaction.setCustomerId(value.getAs("CustomerID"));
		}
		if(value.getAs("Date") != null) {
			String date = value.getAs("Date").toString();
			
			if (!date.contains("#")) {
				//SimpleDateFormat parser = new SimpleDateFormat("m/dd/yyyy");
				//transaction.setDate(parser.parse(value.getAs("Date")));
				transaction.setDate(value.getAs("Date"));
			}
			
		}
		if(value.getAs("gross income") != null) {
			transaction.setGrossIncome(value.getAs("gross income"));
		}
		if(value.getAs("gross margin percentage") != null) {
			transaction.setGrossMergin(value.getAs("gross margin percentage"));
		}
		if(value.getAs("Payment") != null) {
			transaction.setPayment(value.getAs("Payment"));
		}
		if(value.getAs("Product line") != null) {
			transaction.setProductLine(value.getAs("Product line"));
		}
		if(value.getAs("Quantity") != null) {
			transaction.setQuantity(value.getAs("Quantity"));
		}
		if(value.getAs("Rating") != null) {
			transaction.setRating(value.getAs("Rating"));
		}
		if(value.getAs("Tax 5%") != null) {
			transaction.setTax(value.getAs("Tax 5%"));
		}
		if(value.getAs("Time") != null) {
			transaction.setTime(value.getAs("Time"));
		}
		if(value.getAs("Total") != null) {
			transaction.setTotal(value.getAs("Total"));
		}
		if(value.getAs("Unit price") != null) {
			transaction.setUnitPrice(value.getAs("Unit price"));
		}
		
		return transaction;
	}

}
