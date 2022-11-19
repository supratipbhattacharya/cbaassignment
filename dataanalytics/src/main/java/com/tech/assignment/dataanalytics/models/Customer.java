/**
 * 
 */
package com.tech.assignment.dataanalytics.models;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
public class Customer implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	@JsonProperty("person_id")
	private String personId;
	
	@JsonProperty("postcode")
	private String postCode;
	
	@JsonProperty("state")
	private String state;
	
	@JsonProperty("gender")
	private String gender;
	
	@JsonProperty("age")
	private int age;
	
	@JsonProperty("account_type")
	private String accountType;
	
	@JsonProperty("loyal_customer")
	private boolean loyalCustomer;

}
