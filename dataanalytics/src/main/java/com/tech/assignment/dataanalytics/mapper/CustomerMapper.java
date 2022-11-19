/**
 * 
 */
package com.tech.assignment.dataanalytics.mapper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import com.tech.assignment.dataanalytics.models.Customer;


/**
 * @author supratip
 *
 */
public class CustomerMapper implements MapFunction<Row, Customer> {


	private static final long serialVersionUID = 1L;

	@Override
	public Customer call(Row value) throws Exception {
		
		Customer customer = new Customer();
		
		if(value.getAs("person_id") != null) {
			customer.setPersonId(value.getAs("person_id"));
		}
		if(value.getAs("postcode") != null) {
			customer.setPostCode(value.getAs("postcode").toString());
		}
		if(value.getAs("account_type") != null) {
			customer.setAccountType(value.getAs("account_type"));
		}
		if(value.getAs("age") != null) {
			customer.setAge(value.getAs("age"));
		}
		if(value.getAs("gender") != null) {
			customer.setGender(value.getAs("gender"));
		}
		if(value.getAs("loyal_customer") != null) {
			customer.setLoyalCustomer(value.getAs("loyal_customer"));
		}
		if(value.getAs("state") != null) {
			customer.setState(value.getAs("state"));
		}
		
		return customer;
	}

}
