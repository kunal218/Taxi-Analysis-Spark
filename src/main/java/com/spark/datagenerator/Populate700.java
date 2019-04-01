package com.spark.datagenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Random;

public class Populate700 {

	public static int count = 0;

	public static void write(TaxiBean taxiBean) {

		try {
			FileWriter writer = new FileWriter(new File("C:\\Users\\GS-2022\\sparkedemo"), true);

			BufferedWriter buffer = new BufferedWriter(writer);

			buffer.write(taxiBean.toString());
			buffer.newLine();
			System.out.println("recorded " + ++count);

			buffer.close();
			writer.close();

		} catch (IOException exception) {

			System.out.println(exception.getMessage());
		}

	}

	public static void main(String[] args) {

		Timestamp t1 = new Timestamp(System.currentTimeMillis());
		Timestamp t2 = new Timestamp(System.currentTimeMillis());

		double fare = 41;
		String weather = "clear";
		int passengers = 0;

		int counter = 0;

		TaxiBean taxiBean1;

		for (int i = 0; i < 200000; i++) {

			if (i <= 50000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 50000 && i <= 120000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 120000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(13);
				}
				if (j > 5 && j <= 10)
					t2.setHours(14);
				if (j > 10 && j <= 15) {
					t2.setHours(13);
					t2.setMinutes(50);
				}
				if (j > 15) {
					t2.setHours(13);
					t2.setMinutes(30);
				}

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 15.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 15.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 25.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 35.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location1" + j, t1, "location2" + j, t2, passengers, fare,
						"noida", "maharashtra", "India", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

		System.out.println(t1);
		System.out.println(t2);

//weather = cloudy,windy,rainy,clear-------------------------------------------------------------------------------------------------------------

		for (int i = 0; i < 200000; i++) {

			if (i <= 60000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 60000 && i <= 100000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 100000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(13);
				}
				if (j > 5 && j <= 10)
					t2.setHours(14);
				if (j > 10 && j <= 15) {

					t2.setHours(13);
					t2.setMinutes(50);
				}

				if (j > 15) {
					t2.setHours(14);
					t2.setMinutes(30);
				}

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 10.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 10.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 20.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 30.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location1" + j, t1, "location2" + j, t2, passengers, fare,
						"haryana", "punjab", "India", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

//-------------------------------------------------------------------------------------------------------------------------------------------------------

		for (int i = 0; i < 200000; i++) {

			if (i <= 30000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 30000 && i <= 130000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 130000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(13);
				}
				if (j > 5 && j <= 10)
					t2.setHours(14);
				if (j > 10 && j <= 15) {
					t2.setHours(13);
					t2.setMinutes(50);
				}
				if (j > 15) {
					t2.setHours(13);
					t2.setMinutes(30);
				}

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 10.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 10.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 20.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 30.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location3" + j, t1, "location4" + j, t2, passengers, fare,
						"kolkata", "gujarat", "India", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------

		for (int i = 0; i < 200000; i++) {

			if (i <= 70000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 70000 && i <= 130000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 130000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(13);
				}
				if (j > 5 && j <= 10)
					t2.setHours(14);
				if (j > 10 && j <= 15) {
					t2.setHours(13);
					t2.setMinutes(50);
				}
				if (j > 15) {
					t2.setHours(14);
					t2.setMinutes(30);
				}

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 10.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 10.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 20.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 30.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location1" + j, t1, "location2" + j, t2, passengers, fare,
						"zurich", "zurich", "switzerland", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------	

		for (int i = 0; i < 200000; i++) {

			if (i <= 70000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 70000 && i <= 100000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 100000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(13);
				}
				if (j > 5 && j <= 10)
					t2.setHours(14);
				if (j > 10 && j <= 15) {
					t2.setHours(13);
					t2.setMinutes(55);
				}
				if (j > 15) {
					t2.setHours(14);
					t2.setMinutes(35);
				}

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 10.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 10.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 20.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 30.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location" + j, t1, "location1" + j, t2, passengers, fare,
						"chicago", "illinois", "usa", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

//=====================================================================================================================================================================

		for (int i = 0; i < 200000; i++) {

			if (i <= 50000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 50000 && i <= 100000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 100000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(13);
				}
				if (j > 5 && j <= 10)
					t2.setHours(14);
				if (j > 10 && j <= 15)
					t2.setMinutes(57);
				if (j > 15)
					t2.setMinutes(27);

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 10.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 10.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 20.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 30.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location1" + j, t1, "location2" + j, t2, passengers, fare,
						"florida", "california", "usa", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------

		for (int i = 0; i < 200000; i++) {

			if (i <= 20000) {
				t1.setYear(118);
				t2.setYear(118);
			}
			if (i > 20000 && i <= 35000) {

				t1.setYear(117);
				t2.setDate(117);
			}
			if (i > 35000) {
				t1.setYear(116);
				t2.setYear(116);
			}

			for (int j = 0; j < 20; j++) {

				if (j < 5) {
					t2.setHours(17);
				}
				if (j > 5 && j <= 10)
					t2.setHours(18);
				if (j > 10 && j <= 15)
					t2.setMinutes(50);
				if (j > 15)
					t2.setMinutes(30);

				counter++;
				if (counter == 1) {

					t1.setMonth(0);
					t2.setMonth(0);
				} else if (counter == 2) {

					t1.setMonth(1);
					t2.setMonth(1);
				} else if (counter == 3) {
					t1.setMonth(2);
					t2.setMonth(2);
				} else if (counter == 4) {
					t1.setMonth(3);
					t2.setMonth(3);
				} else if (counter == 5) {
					t1.setMonth(4);
					t2.setMonth(4);
				} else if (counter == 6) {
					t1.setMonth(5);
					t2.setMonth(5);
				} else if (counter == 7) {
					t1.setMonth(6);
					t2.setMonth(6);
				} else if (counter == 8) {
					t1.setMonth(7);
					t2.setMonth(7);
				} else if (counter == 9) {
					t1.setMonth(8);
					t2.setMonth(8);
				} else if (counter == 10) {
					t1.setMonth(9);
					t2.setMonth(9);
				} else if (counter == 11) {
					t1.setMonth(10);
					t2.setMonth(10);
				} else if (counter == 12) {
					t1.setMonth(11);
					t2.setMonth(11);
				} else
					counter = 0;

				if (passengers <= 4) {

					if (passengers == 1) {
						weather = "clear";
						fare = 10.0;

					} else if (passengers == 2) {
						weather = "windy";
						fare = 10.0 + 10.0;
					} else if (passengers == 3) {
						weather = "rainy";
						fare = 20.0 + 20.0;
					} else if (passengers == 4) {
						weather = "cloudy";
						fare = 30.0 + 30.0;
					}

					passengers++;

				}

				else {
					passengers = 0;
				}

				Random random = new Random();

				int ratings = random.nextInt(5);

				if (ratings == 0)
					ratings = 5;

				taxiBean1 = new TaxiBean("driver" + j, "location1" + j, t1, "location2" + j, t2, passengers, fare,
						"boston", "Massachusetts", "usa", weather, ratings);

				Populate700.write(taxiBean1);

			}
		}

	}

}
