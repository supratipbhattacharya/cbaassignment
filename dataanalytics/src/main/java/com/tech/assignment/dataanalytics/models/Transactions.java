/**
 * 
 */
package com.tech.assignment.dataanalytics.models;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author supratip
 *
 */

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class Transactions implements Serializable {

	
	private static final long serialVersionUID = 1L;
	
	@JsonProperty("CustomerID")
	private String customerId;
	
	@JsonProperty("Product line")
	private String productLine;
	
	@JsonProperty("Unit price")
	private double unitPrice;
	
	@JsonProperty("Quantity")
	private int quantity;
	
	@JsonProperty("Tax 5%")
	private double tax;
	
	@JsonProperty("Total")
	private double total;
	
	@JsonProperty("Date")
	private String date;
	
	@JsonProperty("Time")
	private String time;
	
	@JsonProperty("Payment")
	private String payment;
	
	@JsonProperty("cogs")
	private double cogs;
	
	@JsonProperty("gross margin percentage")
	private double grossMergin;
	
	@JsonProperty("gross income")
	private double grossIncome;
	
	@JsonProperty("Rating")
	private double rating;

}
