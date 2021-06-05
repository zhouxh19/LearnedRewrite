package hr.fer.zemris.calcite.jdbcproxy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class Main {

	/*
	 * Argument is a model.json file
	 */
	public static void main (String[] argv) throws Exception {

		Class.forName(org.apache.calcite.jdbc.Driver.class.getName());
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		info.setProperty("model", argv[0]);
		Connection calConnection = DriverManager.getConnection("jdbc:calcite:", info);
	}
}
