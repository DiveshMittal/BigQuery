import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.api.gax.rpc.ServerStream;


/*
* Use Google Bigtable to store and analyze sensor data.
*/
public class Bigtable {
	// TODO: Fill in information for your database
	public final String projectId =  "ordinal-rig-426218-g3";
	public final String instanceId = "iitjdb1504";
	public final String COLUMN_FAMILY = "sensor";
	public final String tableId = "weather"; // TODO: Must change table name if sharing my database

	public BigtableDataClient dataClient;
	public BigtableTableAdminClient adminClient;

	public static void main(String[] args) throws Exception {
		Bigtable testbt = new Bigtable();
		testbt.run();
	}

	public void connect() throws IOException {

		// Creates the settings to configure a bigtable data client.
		BigtableDataSettings settings = BigtableDataSettings.newBuilder().setProjectId(projectId)
				.setInstanceId(instanceId).build();

		// Creates a bigtable data client.
		dataClient = BigtableDataClient.create(settings);

		// Creates the settings to configure a bigtable table admin client.
		BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder().setProjectId(projectId)
				.setInstanceId(instanceId).build();

		// Creates a bigtable table admin client.
		adminClient = BigtableTableAdminClient.create(adminSettings);
	}

	public void run() throws Exception {
		connect();
		System.out.println("Connection Successful");
		// TODO: Comment or uncomment these as you proceed. Once load data, comment them
		// out.
		
		//deleteTable();
		//createTable();
		loadData();
		int temp = query1();
		System.out.println("Temperature: " + temp);
		int windspeed = query2();
		System.out.println("Windspeed: " + windspeed);
		ArrayList<Object[]> data = query3();
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.size(); i++) {
			Object[] vals = data.get(i);
			for (int j = 0; j < vals.length; j++)
				buf.append(vals[j].toString() + " ");
			buf.append("\n");
		}
		System.out.println(buf.toString());
		temp = query4();
		System.out.println("Temperature: " + temp);
		close();
	}

	/**
	 * Close data and admin clients
	 */
	public void close() {
		dataClient.close();
		adminClient.close();
	}

	public void createTable() {
		// TODO: Create a table to store sensor data.
		if (!adminClient.exists(tableId)) {
			System.out.println("Creating table: " + tableId);
			CreateTableRequest createTableRequest = CreateTableRequest.of(tableId).addFamily(COLUMN_FAMILY);
			adminClient.createTable(createTableRequest);
			System.out.printf("Table %s created successfully%n", tableId);
		}
	}

	/**
	 * Loads data into database.
	 * Data is in CSV files. Note that must convert to hourly data.
	 * Take the first reading in a hour and ignore any others.
	 */

	public void loadData() throws Exception {
		String path = "bin/data/";
		File folder = new File(path);
		File[] listOfFiles = folder.listFiles();
		// TODO: Load data from CSV files into sensor table
		// Note: There are multiple different ways that you can decide on how to
		// organize this data into columns
		try {
			for (File file : listOfFiles) {
				if (file.isFile()) {
					BufferedReader br = new BufferedReader(new FileReader(file));
					String line;
					String filename = file.getName();
					String Fname = filename.substring(0, filename.length() - 4);
					while ((line = br.readLine()) != null) {
						String[] fields = line.split(",");
						String rowKey = Fname + "#" + fields[1] + "#" + fields[2]; // Assuming the row key is composed
																					// of Pseudo-Julian-Date, Date, and
																					// Time

						RowMutation rowMutation = RowMutation.create(tableId, rowKey)
								.setCell(COLUMN_FAMILY, "Pseudo-Julian-Date", fields[0])
								.setCell(COLUMN_FAMILY, "Date", fields[1]).setCell(COLUMN_FAMILY, "Time", fields[2])
								.setCell(COLUMN_FAMILY, "Temperature", fields[3])
								.setCell(COLUMN_FAMILY, "Dewpoint", fields[4])
								.setCell(COLUMN_FAMILY, "Relhum", fields[5]).setCell(COLUMN_FAMILY, "Speed", fields[6])
								.setCell(COLUMN_FAMILY, "Gust", fields[7])
								.setCell(COLUMN_FAMILY, "Pressure", fields[8]);

						dataClient.mutateRow(rowMutation);

					}
					// System.out.println("Data for :" + filename);
					br.close();
					System.out.println("Data for :" + file);
				}
			}
		} catch (Exception e) {
			throw new Exception(e);
		}
	}

	/**
	 * Query returns the temperature at Vancouver on 2022-10-01 at 10 a.m.
	 *
	 * @return
	 *         ResultSet
	 * @throws SQLException
	 *                      if an error occurs
	 */

		
	public int query1() throws Exception {
		// TODO: Write query to get temperature
		System.out.println("Executing query1.");
		String targetRowKey = "vancouver#2022-10-01#10:00";
		Row retrievedRow = dataClient.readRow(tableId, targetRowKey);
		int temperatureValue = 0;
	
		// Check if the row is not null and has cells
		if (retrievedRow != null && !retrievedRow.getCells().isEmpty()) {
			for (RowCell retrievedCell : retrievedRow.getCells(COLUMN_FAMILY, "Temperature")) {
				// Assuming the temperature is stored as an integer
				temperatureValue = Integer.parseInt(retrievedCell.getValue().toStringUtf8());
				break; // Assuming only one temperature value per row
			}
		} else {
			System.out.println("No data found for the specified row key: " + targetRowKey);
		}
		return temperatureValue;
	}
	
	
	/**
	 * Query returns the highest wind speed in the month of September 2022 in
	 * Portland.
	 *
	 * @return
	 *         ResultSet
	 * @throws SQLException
	 *                      if an error occurs
	 */
		
	public int query2() throws Exception {
		// TODO: Write query to find max wind speed
		System.out.println("Executing query #2");
		int maxWindSpeed = 0;
		Query windData = Query.create(tableId).prefix("portland#2022-09")
				.filter(Filters.FILTERS.qualifier().regex("Speed"));
		ServerStream<Row> rows = dataClient.readRows(windData);
		for (Row row : rows) {
			for (RowCell cell : row.getCells(COLUMN_FAMILY, "Speed")) {
				int currentWind = Integer.parseInt(cell.getValue().toStringUtf8());
				if (currentWind > maxWindSpeed) {
					maxWindSpeed = currentWind;
				}
			}
		}
		return maxWindSpeed;
	}
	
	public ArrayList<Object[]> query3() throws Exception {
		System.out.println("Executing query3.");
		// Create a query to filter rows by prefix
		Query weatherQuery = Query.create(tableId).prefix("seatac#2022-10-02");
	
		ArrayList<Object[]> weatherData = new ArrayList<>();
		ServerStream<Row> resultRows = dataClient.readRows(weatherQuery);
	
		for (Row weatherRow : resultRows) {
			Object[] weatherReading = new Object[8];
			weatherReading[0] = weatherRow.getKey().toStringUtf8().split("#")[1]; // Date
			weatherReading[1] = weatherRow.getKey().toStringUtf8().split("#")[2]; // Time
	
			for (RowCell weatherCell : weatherRow.getCells()) {
				String cellQualifier = weatherCell.getQualifier().toStringUtf8();
				String cellValue = weatherCell.getValue().toStringUtf8();
	
				switch (cellQualifier) {
					case "Temperature":
						weatherReading[2] = Integer.parseInt(cellValue);
						break;
					case "Dewpoint":
						weatherReading[3] = Integer.parseInt(cellValue);
						break;
					case "Relhum":
						weatherReading[4] = cellValue;
						break;
					case "Speed":
						weatherReading[5] = cellValue;
						break;
					case "Gust":
						weatherReading[6] = cellValue;
						break;
					case "Pressure":
						weatherReading[7] = cellValue;
						break;
				}
			}
			weatherData.add(weatherReading);
		}
		return weatherData;
	}

	/**
	 * Query returns the highest temperature at any station in the summer months
	/ of
	 * 2022 (July (7), August (8)).
	 *
	 * @return
	 * ResultSet
	 * @throws SQLException
	 * if an error occurs
	 */
	
	
	 
	 
	 public int query4() throws Exception {
     // TODO: Write query to find max temperature
     // Try to avoid reading the entire table. Consider using readRowRanges() instead.
		System.out.println("Executing query4.");
	
		Query seatacQuery = Query.create(tableId).range("seatac#2022-07", "seatac#2022-08");
		Query vancouverQuery = Query.create(tableId).range("vancouver#2022-07", "vancouver#2022-08");
		Query portlandQuery = Query.create(tableId).range("portland#2022-07", "portland#2022-08");
	
		int highestTemperature = -100;
	
		ServerStream<Row> seatacRows = dataClient.readRows(seatacQuery);
		for (Row seatacRow : seatacRows) {
			for (RowCell temperatureCell : seatacRow.getCells(COLUMN_FAMILY, "Temperature")) {
				int temperatureValue = Integer.parseInt(temperatureCell.getValue().toStringUtf8());
				if (temperatureValue > highestTemperature) {
					highestTemperature = temperatureValue;
				}
			}
		}
	
		ServerStream<Row> vancouverRows = dataClient.readRows(vancouverQuery);
		for (Row vancouverRow : vancouverRows) {
			for (RowCell temperatureCell : vancouverRow.getCells(COLUMN_FAMILY, "Temperature")) {
				int temperatureValue = Integer.parseInt(temperatureCell.getValue().toStringUtf8());
				if (temperatureValue > highestTemperature) {
					highestTemperature = temperatureValue;
				}
			}
		}
	
		ServerStream<Row> portlandRows = dataClient.readRows(portlandQuery);
		for (Row portlandRow : portlandRows) {
			for (RowCell temperatureCell : portlandRow.getCells(COLUMN_FAMILY, "Temperature")) {
				int temperatureValue = Integer.parseInt(temperatureCell.getValue().toStringUtf8());
				if (temperatureValue > highestTemperature) {
					highestTemperature = temperatureValue;
				}
			}
		}
	
		return highestTemperature;
	 }


	/**
	* Create your own query and test case demonstrating some different.
	*
	* @return
	* ResultSet
	* @throws SQLException
	* if an error occurs
	*/
		


	/**
	 * Delete the table from Bigtable.
	 */

	public void deleteTable() {
		System.out.println("\nDeleting table: " + tableId);
		try {
			adminClient.deleteTable(tableId);
			System.out.printf("Table %s deleted successfully%n", tableId);
		} catch (NotFoundException e) {
			System.err.println("Failed to delete a non-existent table: " + e.getMessage());
		}
	}
}