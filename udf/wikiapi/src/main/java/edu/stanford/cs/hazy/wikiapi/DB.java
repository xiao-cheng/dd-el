package edu.stanford.cs.hazy.wikiapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DB {

  public static Connection getConnection() throws SQLException{
    String port = System.getenv("PGPORT");
    if (port == null){
      port = "8432";
    }
    String dbUrl = String.format("jdbc:postgresql://localhost:%s/entitylinking", port);
    return DriverManager.getConnection(dbUrl);
  }
  
}
