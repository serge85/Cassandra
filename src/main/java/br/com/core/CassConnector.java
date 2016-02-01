package br.com.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassConnector {
	
	private static Cluster cluster;
	private static Session session;
	
	public static Cluster getConnection(String node){
		return Cluster.builder().addContactPoint(node).build();
	}
	
	public static void main(String[] args){
		
		cluster = getConnection("localhost");
		session = cluster.connect();
		
		session.execute("CREATE KEYSPACE myKs WITH REPLICATION = "
				+ "{'class': 'SimpleStrategy', 'replication_factor': 1};");
		
		
		session.execute("USE myKs");
		
		System.out.println("Cassandra Connected!");
		
		session.close();
		cluster.close();
		
		
	}

}
