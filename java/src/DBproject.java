/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import java.lang.Boolean;
import java.util.Scanner;
import java.lang.Character;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("\nMAIN MENU");
				System.out.println("---------");
				System.out.println("0: Testing Tables");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 0: Test(esql); break;
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void Test(DBproject esql) {
		try{
			int i = 0;
			ArrayList<String> a = new ArrayList<String>();
			String query1 = "Select * From Plane;";
			a.add(query1);
			String query2 = "Select * From Pilot;";
			a.add(query2);
			String query3 = "Select * From Flight;";
			a.add(query3);
			String query4 = "Select * From Technician;";
			a.add(query4);
			String query5 = "Select * From Reservation";
			a.add(query5);

			System.out.println("\n1: Plane table");
			System.out.println("2: Pilot table");
			System.out.println("3: Flight table");
			System.out.println("4: Technician table");
			System.out.println("5: Reservation table");
			System.out.println("\nEnter which table to look at:");
			
			i = readInt();

			while(i < 0 && i > 5){
				System.out.println("Input is not a selection, try again: ");
				i = readInt();
			}
			
			int rowCount = esql.executeQueryAndPrintResult(a.get(i-1));
			System.out.println(rowCount);
		
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}



	public static void AddPlane(DBproject esql) {//1
		try{
			int age = -1, seats = 0;
			String make, model;

			System.out.println("\tEnter make of the plane: ");
				make = readString();
			System.out.println("\tEnter model of the plane: ");
				model = readString();
			System.out.println("\tEnter the age of the plane: ");
				age = readInt();
				
				while(age < 0){
					System.out.println("\tInput is invalid. Cannot have a negative number. Try again: ");
					age = readInt();
				}
			System.out.println("\tEnter the number of passenger seats the plane can hold (Between 0 and 500): ");
				seats = readInt();

				while(seats < 0 || seats > 500){
					if(seats < 0){
						System.out.println("\tInput is invalid. Number of seats cannot be negative. Try again: ");
						seats = readInt();
					}
					else if(seats > 500){
						System.out.println("\tInput is invalid. Number of seats cannot exceed 500. Try again: ");
						seats = readInt();
					}
				}

			String query = "INSERT INTO Plane (make, model, age, seats) VALUES ";
			query += "('" + make + "', '" + model + "', " + age + ", " + seats + ");";
			

			//System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddPilot(DBproject esql) {//2
		try{
			String name, nation;

			System.out.println("\tEnter pilot's full name: ");
				name = readString();
			System.out.println("\tEnter pilot's nationality: ");
				nation = readString();


			String query = "INSERT INTO Pilot (fullname, nationality) VALUES ";
			query += "( '" + name + "', '" + nation + "');";


			//System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}

	public static void AddFlight(DBproject esql) {//3
		// Given a pilot, plane and flight, adds a flight in the DB
		try{
			int cost = 0, num_sold = 0, num_stops = 0;
			String depart_date, arrive_date, a_airport, d_airport;

			System.out.println("\tEnter flight cost (a number greater than 0): ");
				cost = readInt();

				while(cost <= 0){
					System.out.println("\tInput is invalid. Cost cannot be less than or equal to 0. Try again: ");
					cost = readInt();
				}
	
			System.out.println("\tEnter the number of seats sold: ");
				num_sold = readInt();

				while(num_sold < 0){
					System.out.println("\tInput is invalid. Number of seats sold cannot be less than 0. Try again: ");
					num_sold = readInt();
				}

			System.out.println("\tEnter the number of stops the flight has: ");
				num_stops = readInt();

				while(num_stops < 0){
					System.out.println("\tInput is invalid. Number of stops cannot be less than 0. Try again: ");
					num_stops = readInt();
				}

			System.out.println("\tEnter the departure date (YYYY-MM-DD): ");
				depart_date = readString();

				while(dateCheck(depart_date)){
					depart_date = readString();
				}

			System.out.println("\tEnter the arrival date (YYYY-MM-DD): ");
				arrive_date = readString();

				while(dateCheck(arrive_date)){
					arrive_date = readString();
				}

			System.out.println("\tEnter the code for the arrival airport (5 letters): ");
				a_airport = readString();

				while(codeCheck(a_airport)){
					a_airport = readString();
				}

			System.out.println("\tEnter the code for the departure airport (5 letters): ");
				d_airport = readString();

				while(codeCheck(d_airport)){
					d_airport = readString();
				}

			String query = "INSERT INTO Flight (cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_airport, departure_airport) VALUES ";
			query += "(" + cost + ", " + num_sold + ", " + num_stops + ", cast('" + depart_date + "' as date), cast('" + arrive_date + "' as date), '" + a_airport.toUpperCase() + "', '" + d_airport.toUpperCase() + "');";
			
			//System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void AddTechnician(DBproject esql) {//4
		try{
			String name;

			System.out.println("\tEnter full name of technician: ");
				name = readString();

			String query = "INSERT INTO Technician (full_name) VALUES ";
			query += "('" + name + "');";

			//System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void BookFlight(DBproject esql) {//5
		// Given a customer and a flight that he/she wants to book, add a reservation to the DB
		try{
			int fid = 0, x = 0;
			String first, last, ddate, query, q1, q2;
			char stat, c;

			System.out.println("\tEnter first name: ");
			first = readString();
			System.out.println("\tEnter last name: ");
			last = readString();

			System.out.println("\tDo you know the flight number: (Y/N)?");
			c = readChar();


			while(c != 'y' && c != 'n' && c != 'Y' && c != 'N'){
				System.out.println("\tInput is invalid, try again: ");
				c = readChar();
			}

			if(c == 'y'){
				System.out.println("Enter flight number: ");
				fid = readInt();

				q1 = "THEN INSERT INTO Reservation (cid, fid, status) VALUES ((Select id from Customer Where fname = '" + first + "' and lname = '" + last + "'), " + fid + ", 'C');";
				q2 = " ELSE INSERT INTO Reservation (cid, fid, status) VALUES ((Select id from Customer Where fname = '" + first + "' and lname = '" + last + "'), " + fid + ", 'W'); END IF; END $$;";
				query = "DO $$ BEGIN IF (Select P.seats From Plane P, FlightInfo I Where I.plane_id = P.id and I.flight_id = " + fid + ") > (Select F.num_sold From Flight F, FlightInfo I Where F.fnum = I.flight_id and F.fnum = " + fid + ") ";

				query += q1 + q2;

				esql.executeUpdate(query);
			} else {
				System.out.println("Enter departure date (YYYY-MM-DD): ");
				ddate = readString();


				while(dateCheck(ddate)){
					ddate = readString();
				}

				query = "Select * From Flight Where actual_departure_date::date >= '" + ddate + "' and actual_departure_date < ('"  + ddate + "'::date + '1 day'::interval);";
				x = esql.executeQueryAndPrintResult(query);

				System.out.println("Enter the flight number of the desired flight: ");
				fid = readInt();

				q1 = "THEN INSERT INTO Reservation (cid, fid, status) VALUES ((Select id from Customer Where fname = '" + first + "' and lname = '" + last + "'), " + fid + ", 'C');";
				q2 = " ELSE INSERT INTO Reservation (cid, fid, status) VALUES ((Select id from Customer Where fname = '" + first + "' and lname = '" + last + "'), " + fid + ", 'W'); END IF; END $$;";
				query = "DO $$ BEGIN IF (Select P.seats From Plane P, FlightInfo I Where I.plane_id = P.id and I.flight_id = " + fid + ") > (Select F.num_sold From Flight F, FlightInfo I Where F.fnum = I.flight_id and F.fnum = " + fid + ") ";

				query += q1 + q2;

				esql.executeUpdate(query);
			}
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
		try{
			int fid = 0;
			String ddate, query;
			char c;


			System.out.println("Do you know flight number? (Y/N): ");
			c = readChar();

			while(c != 'y' && c != 'n' && c != 'Y' && c != 'N'){
				System.out.println("\tInput is invalid, try again: ");
				c = readChar();
			}


			if(c == 'y'){
				//System.out.println("\nWorked\n");
				System.out.println("\tEnter flight number: ");
				fid = readInt();

				query = "Select (P.seats - F.num_sold) as \"Available Seats\" From Plane P, Flight F, FlightInfo I Where P.id = I.plane_id and F.fnum = I.flight_id and F.fnum = " + fid + ";";

				int x = esql.executeQueryAndPrintResult(query);
				System.out.println("\n");	
			} else {
				//System.out.println("\nFailed\n");
				System.out.println("\tEnter flight departure date (YYYY-MM-DD): ");
				ddate = readString();

				while(dateCheck(ddate)){
					ddate = readString();
				}

				query = "Select * From Flight Where actual_departure_date::date >= '" + ddate + "' and actual_departure_date < ('"  + ddate + "'::date + '1 day'::interval);";
				int x = esql.executeQueryAndPrintResult(query);

				System.out.println("\nEnter the flight number of the desired flight: ");
				fid = readInt();

				query = "Select (P.seats - F.num_sold) as \"Available Seats\" From Plane P, Flight F, FlightInfo I Where P.id = I.plane_id and F.fnum = I.flight_id and F.fnum = " + fid + ";";

				x = esql.executeQueryAndPrintResult(query);
			}
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
		try{
			String query = "Select R.plane_id, COUNT(*) as n From Repairs R Group by R.plane_id Order by n desc;";

			int x = esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
		try{
			String query = "Select extract(year from repair_date) as Year, COUNT(*) as Sum From Repairs Group by Year Order by Sum;";

			int x = esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
		try{
			int fid = 0;
			char stat, c;
			String ddate;

			System.out.println("Do you know flight number? (y/n): ");
			c = readChar();

			while(c != 'y' && c != 'n'){
				System.out.println("\tInput is invalid, try again: ");
				c = readChar();
			}


			if(c == 'y'){
				//System.out.println("\nWorked\n");
				System.out.println("\tEnter flight number: ");
				fid = readInt();
				System.out.println("\tEnter the passenger status (W, R, C): ");
				stat = readChar();

				while(stat != 'W' && stat != 'w' && stat != 'R' && stat != 'r' && stat != 'C' && stat != 'c'){
					System.out.println("Input is not a status, try again: ");
					stat = readChar();
				}

				String query = "Select COUNT(*) From Flight F, Reservation R Where F.fnum = " + fid + " and R.status = '" + stat + "' and F.fnum = R.fid;";

				int x = esql.executeQueryAndPrintResult(query);
				System.out.println("\n");	
			} else {
				//System.out.println("\nFailed\n");
				System.out.println("\tEnter flight departure date (YYYY-MM-DD): ");
				ddate = readString();

				while(dateCheck(ddate)){
					ddate = readString();
				}

				String query = "Select * From Flight Where actual_departure_date::date >= '" + ddate + "' and actual_departure_date < ('"  + ddate + "'::date + '1 day'::interval);";
				int x = esql.executeQueryAndPrintResult(query);

				System.out.println("\nEnter the flight number of the desired flight: ");
				fid = readInt();


				System.out.println("\tEnter the passenger status (W, R, C): ");
				stat = readChar();

				while(stat != 'W' && stat != 'w' && stat != 'R' && stat != 'r' && stat != 'C' && stat != 'c'){
					System.out.println("Input is not a status, try again: ");
					stat = readChar();
				}

				query = "Select COUNT(*) From Flight F, Reservation R Where F.fnum = " + fid + " and R.status = '" + stat + "' and F.fnum = R.fid;";

				x = esql.executeQueryAndPrintResult(query);
			}
		}catch(Exception e){
			System.err.println(e.getMessage());
		}
	}

	//Added functions
	public static int readInt() {
		int input;
		// returns only if a correct value is given.
		do {
			//System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readInt

	public static String readString() {
		String input;
		// returns only if a correct value is given.
		do {
			//System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = in.readLine();
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readString
	public static char readChar() {
		Scanner s = new Scanner(System.in);
		char input;
		// return only if a correct value in given.
		do {
			//System.out.print("Please make your choice: ");
			try { //read the integer, parse it and break.
				input = s.next().charAt(0);
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChar
	public static boolean dateCheck(String dateIn) {
		//checks data input length
		if(dateIn.length() != 10){
			System.out.println("\tInput does not meet specification (YYYY-MM-DD). Try again: ");
			return true;
		}
		//checks date input structure
		if(dateIn.charAt(4) != '-' || dateIn.charAt(7) != '-'){
			System.out.println("\tInput does not meet specification (YYYY-MM-DD). Try again: ");
			return true;
		}
		//checks if input entered are integers
		for(int i = 0; i < dateIn.length(); i++){
			//System.out.println("Entered here: " + i);		
			if(i == 4 || i == 7){i += 1;}
			else if(!(Character.isDigit(dateIn.charAt(i)))){
				System.out.println("\tInput does not meet specification (YYYY-MM-DD). Try again: ");
				return true;
			}
		}
		return false;
	}//end dateCheck
	public static boolean codeCheck(String code) {
		if(code.length() != 5){
			System.out.println("\tInput does not meet specification. Try again: ");
			return true;
		}
		for(int i = 0; i < code.length(); i++){
			if(!(Character.isLetter(code.charAt(i)))){
				System.out.println("\tInput does not meet specification. Try again: ");
				return true;
			}
		}
		return false;
	}
}
