import java.io.*;
import java.util.*;
import java.sql.*;

public class Practical3 {
	
	public static String[] propStore = new String[3]; //stores properties to connect to server
	public static final int COLUMNS = 13; //amount of columns in graph
	public static String[] lineStore = new String[COLUMNS]; //stores separated line from data to be turned into SQL-syntactically correct String

	public static void readProperties() throws Exception { //Reads data from specified properties folder
		try {
			System.out.println();
			System.out.println("Reading properties...");
			FileInputStream propInput = new FileInputStream("database.properties");
			
			Properties props = new Properties();
			props.load(propInput); //stores semi-structured data by attribute and value following '='
			propInput.close();
			
			String host = props.getProperty("host");  //used W06 example, stores value for each property in quotes
	        String port = props.getProperty("port");
	        String username = props.getProperty("username");
	        propStore[0] = username;
	        String password = props.getProperty("password");
	        propStore[1] = password;
	        String db = props.getProperty("db");
	        
	        StringBuilder URLBuild = new StringBuilder(); //puts read properties into readable URL for database
	        URLBuild.append("jdbc:mysql://");
	        URLBuild.append(host);
	        URLBuild.append(":");
	        URLBuild.append(port);
	        URLBuild.append("/");
	        URLBuild.append(db);
	        String URL = URLBuild.toString();
	        propStore[2] = URL;
	        
	        System.out.println();
	        System.out.println("Properties read");
		}
		catch (FileNotFoundException e) {  //catches if file isn't where it should be
			System.out.println("File not found, check to make sure file is in correct directory");
			System.exit(0);
		}
		catch (IOException e) { //catches if error arises during input
			System.out.println("Error: file cannot be input");
		}
		catch (Exception e) { //catches any unknown errors
			System.out.println("Error: File cannot be read");
		}
	}
	
	public static void tableInitializer(Connection connection) throws SQLException { //deletes any table labelled practical3 and sets attributes(columns) for new one
		Statement dropTable = null;
		Statement newTable = null; 
		try {
			dropTable = connection.createStatement(); //deletes table if already exists
			dropTable.executeUpdate("DROP TABLE IF EXISTS practical3;");
			newTable = connection.createStatement();
			StringBuilder longUpdate = new StringBuilder(); //I didn't realize you had to put all your input into one executeUpdate so I split it to make it easy to read
			longUpdate.append("CREATE TABLE practical3( sample_id INT, record_submit_time DATETIME, ");
			longUpdate.append("sample_holderno INT, sample_duration TIME, exp_id INT, exp_name VARCHAR(20), ");
			longUpdate.append("exp_description VARCHAR(100), user_id INT, group_abbr VARCHAR(5), ");
			longUpdate.append("solvent_abbr VARCHAR(8), spectrometer_id INT, spectrometer_name VARCHAR(6), spectrometer_capacity INT);");
			String update = longUpdate.toString();
			newTable.executeUpdate(update); //inputs update String to create table with attributes
			System.out.println();
			System.out.println("Table initialized");
		}
		catch (SQLException e) {
			if (e.getErrorCode() == 1064) { //used to make sure mysql syntax was correct
				System.out.println("Error: SQL table initialization syntax is incorrect");
			}
			else {
				System.out.println("Error: SQL could not initialize table");
			}
		}
		catch (Exception e) {
			System.out.println("Error: " +e.toString());
		}
		finally {
			if (newTable != null) {
				dropTable.close();
				newTable.close();
			}
		}
	}
	
	public static void readData(Connection connection) throws SQLException { //reads data from user-specified file, converts each line into correct syntax for mysql, then stores it into mysql
		Statement inputData = null;
		
		try {
			inputData = connection.createStatement();
			System.out.println();
			System.out.print("Please type name of file to be read: ");
			Scanner consoleReader = new Scanner(System.in);
			String fileName = consoleReader.next(); //user inputs file name to be read
			System.out.println();
			BufferedReader dataReader = new BufferedReader(new FileReader(fileName));
			consoleReader.close();
			String contents = null;
			int index = 3;
			for (int i = 0; i < index; i++) {
				if ((contents = dataReader.readLine()) != null) {
					lineStore = contents.split(",");
					StringBuilder SQLLine = new StringBuilder();
					SQLLine.append("INSERT INTO practical3 VALUES(");
					System.out.print("\rStoring Data from line: " +i); //Clean until inputting large data sets, then becomes inconsistently clean
					for (int j = 0; j < COLUMNS; j++) {
						if (lineStore[j].equals("")) { //If there is an empty string between two commas, error in the csv file
							throw new NullPointerException();
						}
						if (j != (lineStore.length -1)) {
							if (j == 1 && i != 0) {
								String[] date_time = new String[1];  //the following brings the year to the front for proper mysql storage
								date_time = lineStore[1].split(" ");
								String[] date = new String[2];
								date = date_time[0].split("/");
								StringBuilder correctOrderDateTime = new StringBuilder();
								correctOrderDateTime.append(date[2]+"/"+date[1]+"/"+date[0]+" "+date_time[1]);
								lineStore[j] = correctOrderDateTime.toString();								
							}
							if (j != 1 && j != 3 && j != 5 && j!= 6 && j != 8 && j != 9 && j != 11) {
								lineStore[j] = lineStore[j] +", "; 
								SQLLine.append(lineStore[j]);
							}
							else {
								lineStore[j] = "'" +lineStore[j] +"', "; //puts VARCHARS in quotes
								SQLLine.append(lineStore[j]);
							}
						}
						else {
							SQLLine.append(lineStore[j]); //stores without a comma because last value of line
						}
						
					}
					SQLLine.append(")");
					String wholeLine = SQLLine.toString();
					if (i != 0) {
						inputData.executeUpdate(wholeLine); //actual input of data
					}
					index++;
				}
				else {
					index = i; //closes arraylist length to correct length
				}
			}
			System.out.println();
			System.out.println();
			System.out.println("Data stored & formatted");
		}
		catch (SQLException e) {
			if (e.getErrorCode() == 1064) {
				System.out.println("Error: SQL table syntax is incorrect");
			}
			else {
				System.out.println("Error inputting data into mySQL: " +e.toString());
			}
		}
		catch (IOException e) { //if incorrect file name is input
			System.out.println("Error reading data from file:" +e.toString());
		}
		catch (NullPointerException e) { //Only throw if file is incorrectly formatted/ cant be read
			System.out.println("Error: file syntax is incorrect");
			System.out.println("Please check to make sure file is in proper csv format");
			System.exit(0);
		}
		catch (Exception e) { //describes error to user
			System.out.println("Error: " +e.toString());
		}
		finally {
			if (inputData != null) {
				inputData.close();
			}
		}
	}
	
	
	public static void printAllRecords(Connection connection) throws SQLException { //Prints all records from practical3 of mysql
		Statement statement = null;
		
        try {
        	System.out.println();
        	System.out.println("Printing all Records...");
    		System.out.println();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM practical3");
            while (resultSet.next()) {
            	int sample_id = resultSet.getInt("sample_id"); //gets individual values for each row of attributes
            	String record_submit_time = resultSet.getString("record_submit_time");
            	int sample_holderno = resultSet.getInt("sample_holderno");
            	String sample_duration = resultSet.getString("sample_duration");
            	int exp_id = resultSet.getInt("exp_id");
            	String exp_name = resultSet.getString("exp_name");
            	String exp_description = resultSet.getString("exp_description");
            	int user_id = resultSet.getInt("user_id");
            	String group_abbr = resultSet.getString("group_abbr");
            	String solvent_abbr = resultSet.getString("solvent_abbr");
            	int spectrometer_id = resultSet.getInt("spectrometer_id");
            	String spectrometer_name = resultSet.getString("spectrometer_name");
            	int spectrometer_capacity = resultSet.getInt("spectrometer_capacity");
            	
            	System.out.println("Sample id: " +sample_id);  //then prints them
            	System.out.println("Record submit time: " +record_submit_time);
            	System.out.println("Sample holder no: "  +sample_holderno);
            	System.out.println("Sample duration: " +sample_duration);
            	System.out.println("Experiment ID: " +exp_id);
            	System.out.println("Experiment Name: "+exp_name);
            	System.out.println("Experiment Description: " +exp_description);
            	System.out.println("User ID: " +user_id);
            	System.out.println("Group Abbreviation: " +group_abbr);
            	System.out.println("Solvent Abbreviation: " +solvent_abbr);
            	System.out.println("Spectrometer ID: " +spectrometer_id);
            	System.out.println("Spectrometer name: " +spectrometer_name);
            	System.out.println("Spectrometer capacity: " +spectrometer_capacity);
            	System.out.println();
            }
            System.out.println("Records printed");
		}
		catch (SQLException e) {
			System.out.println("SQL Exception: Couldn't print from records due to SQL syntax error: " +e.toString());
		}
        catch (Exception e) {
			System.out.println("Error: " +e.toString());
		}
        finally {
			if (statement != null) {
				statement.close();
			}
		}
	}
	
	
	public static void printAlec(Connection connection) throws SQLException { //prints all records where spectrometer 'Alec' was used
		Statement statement = null;
		
        try {
        	System.out.println();
        	System.out.println("Printing Records for 'Alec'...");
    		System.out.println();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM practical3 WHERE spectrometer_name = 'Alec'");
            while (resultSet.next()) {
            	int sample_id = resultSet.getInt("sample_id");
            	String solvent_abbr = resultSet.getString("solvent_abbr");
            	String exp_name = resultSet.getString("exp_name");

            	System.out.println("Sample id: " +sample_id);
            	System.out.println("Solvent Abbreviation: " +solvent_abbr);
            	System.out.println("Experiment Name: "+exp_name);  
            	System.out.println();
            }
        }
        catch (SQLException e) {
        	System.out.println("SQLException: Query syntax is incorrect: " +e.getErrorCode());
        }
        catch (Exception e) {
			System.out.println("Error: " +e.toString());
		}
        finally {
			if (statement != null) {
				statement.close();
			}
		}
	}
	
	
	public static void printProtonCount(Connection connection) throws SQLException { //counts records with experiment name 'proton.a.and' and prints count
		Statement statement = null;
		
        try {
        	System.out.println("Printing Records with experiment name 'proton.a.and'...");
    		System.out.println();
            statement = connection.createStatement();
            statement.executeUpdate("SET sql_mode = '';");
            ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM practical3 WHERE exp_name = 'proton.a.and';");
            int count = 0;
            while (resultSet.next()) {
            	count = resultSet.getInt(1);
            }
            System.out.println("Number of sample run using experiment 'proton.a.and': " +count);
        }
        catch (SQLException e) {
        	System.out.println("SQLException: Query syntax is incorrect: " +e.getErrorCode());
        }
        finally {
			if (statement != null) {
				statement.close();
			}
		}
	}
	
     
	public static void printDurations(Connection connection) throws SQLException { //sorts, calculates, and prints max and min durations
		Statement statement = null;
		
		try {
			System.out.println();
        	System.out.println("Printing durations...");
    		System.out.println();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT sample_duration FROM practical3 ORDER BY sample_duration DESC;");
        	String max_duration = null;
        	String min_duration = null;
        	
            while (resultSet.next()) {
        		max_duration = resultSet.getString(1); //the following takes the string time and puts it into a sortable integer
        	}
        	System.out.println("Max Duration: " +max_duration);
            
        	resultSet = statement.executeQuery("SELECT sample_duration FROM practical3 ORDER BY sample_duration;");
        	
        	while (resultSet.next()) {
        		min_duration = resultSet.getString("sample_duration"); //the following takes the string time and puts it into a sortable integer
        	}
        	System.out.println("Min Duration: " +min_duration);
		}
		catch (SQLException e) {
			System.out.println("SQLException when printing durations: " +e.toString());
		}
		catch (Exception e) {
			System.out.println("Error: " +e.toString());
		}
		finally {
			if (statement != null) {
				statement.close();
			}
		}
	}
	
	public static void accessDB(Connection connection) throws SQLException { //runs all methods within connection
		System.out.println();
		System.out.println("Connecting to database...");
		try {
			connection = DriverManager.getConnection(propStore[2], propStore[0], propStore[1]); //uses property info to connect
			System.out.println();
			System.out.println("Connected");
			tableInitializer(connection); //runs the program using the connection
			readData(connection);
			printAllRecords(connection);
			printAlec(connection);
			printProtonCount(connection);
			printDurations(connection);

		}
		catch (SQLException e) { 
			System.out.println("SQL Exception: Couldn't connect to URL");
		}
		catch (Exception e) {
			System.out.println("Error: " +e.toString());
		}
		finally {
			if (connection != null) {
				connection.close();
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		try {
			readProperties();
			Connection connection = null;
			accessDB(connection);
		}
		catch (Exception e) { //Will never reach this so long as individual method catches function as intended
			System.out.println("Unkown error occurred running program: " +e.toString());
		}
	}

}
