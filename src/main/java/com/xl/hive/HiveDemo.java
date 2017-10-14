package com.xl.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDemo {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {

		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
			Statement stmt = con.createStatement();
			  
			stmt.execute("CREATE DATABASE userdb1");
			System.out.println("Database userdb created successfully.");
     
			con.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
