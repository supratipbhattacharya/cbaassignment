package com.tech.assignment.dataanalytics;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.tech.assignment.dataanalytics.serviceimpl.DataAnalytics;

/**
 * @author supratip
 *
 */

@SpringBootApplication
@EnableBatchProcessing
public class DataanalyticsApplication implements CommandLineRunner {

	@Autowired
	private DataAnalytics dataAnalytics;
	
	public static void main(String[] args) {
		SpringApplication.run(DataanalyticsApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		dataAnalytics.findDataSet();
	}

}
