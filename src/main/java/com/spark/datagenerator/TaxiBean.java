package com.spark.datagenerator;

import java.sql.Timestamp;

public class TaxiBean {

	String taxi_driver_name;
	String pickup_loc;
	Timestamp pickup_ts;
	String drop_loc;
	Timestamp drop_ts;
	int no_of_passengers;
	double fare;
	String city;
	String state;
	String country;
	String weather;
	int customer_review;

	public TaxiBean(String taxi_driver_name, String pickup_loc, Timestamp pickup_ts, String drop_loc, Timestamp drop_ts,
			int no_of_passengers, double fare, String city, String state, String country, String weather,
			int customer_review) {

		this.taxi_driver_name = taxi_driver_name;
		this.pickup_loc = pickup_loc;
		this.pickup_ts = pickup_ts;
		this.drop_loc = drop_loc;
		this.drop_ts = drop_ts;
		this.no_of_passengers = no_of_passengers;
		this.fare = fare;
		this.city = city;
		this.state = state;
		this.country = country;
		this.weather = weather;
		this.customer_review = customer_review;
	}

	public String toString() {
		return taxi_driver_name + "," + pickup_loc + "," + pickup_ts + "," + drop_loc + "," + drop_ts + ","
				+ no_of_passengers + "," + fare + "," + city + "," + state + "," + country + "," + weather + ","
				+ customer_review;
	}

}