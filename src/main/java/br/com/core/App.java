package br.com.core;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Cassandra Application!
 *
 */
public class App {
	private static Cluster cluster;
	private static Session session;

	public static void main(String[] args) {
		cluster = CassConnector.getConnection("localhost");
		session = cluster.connect("demo");

		insertRecord();
		
		
	}

	// Insert one record into the users table
	public static void insertRecord() {

		PreparedStatement statement = session.prepare("INSERT INTO users" + "(id, lastname, age, city, email, firstname)" + "VALUES (now(),?,?,?,?,?);");
		BoundStatement boundStatement = new BoundStatement(statement);
		session.execute(boundStatement.bind("Vieira", 5, "Sao paulo", "francois@example.com", "Francois"));
		
		System.out.println("Inserido");
	}

	// Use select to get the user we just entered
	public static void selectRecord() {
		Statement select = QueryBuilder.select().all().from("demo", "users").where(QueryBuilder.eq("lastname", "Jones"));
		ResultSet results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"), row.getInt("age"));
		}
	}

	// Update the same user with a new age
	// Select and show the change
	public static void UpdateAndSelect(){
		Statement update = QueryBuilder.update("demo", "users").with(QueryBuilder.set("age", 36)).where((QueryBuilder.eq("lastname", "Jones")));
		session.execute(update);
				
		Statement select = QueryBuilder.select().all().from("demo", "users").where(QueryBuilder.eq("lastname", "Jones"));
		ResultSet results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("firstname"), row.getInt("age"));
		}
	}
}
