package edu.stanford.cs.hazy.wikiapi;

import java.sql.*;

/**
 * Stores the parsed result in a SQL database
 * 
 * @author Xiao Cheng
 *
 */
public class Index {

  public static Connection connect(String user, String password, int port,
      String database) {
    Connection c = null;
    try {
      // Load postgres class
      Class.forName("org.postgresql.Driver");
      String url = String
          .format("jdbc:postgresql://localhost:%d/%s", port, database);
      c = DriverManager.getConnection(url, user, password);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
      System.exit(0);
    }
    return c;
  }

  /**
   * Connects to a default localhost postgres database
   * 
   * @return
   */
  public static Connection connect() {
    return connect("xiao", "", 5432, "entitylinking");
  }

  /**
   * 
   * @param c
   * @param article_id
   * @param title
   * @param text
   */
  public static void store(Connection c, String id, String title, String html,
      String mediawiki) {
    PreparedStatement stmt = null;
    try {
      stmt = c
          .prepareStatement("INSERT INTO wiki_pages (id,title,html,mediawiki) values (?,?,?,?)");
      stmt.setString(1, id);
      stmt.setString(2, title);
      stmt.setString(3, html);
      stmt.setString(4, mediawiki);
      stmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (stmt != null)
        try {
          stmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
    }
  }

  public static void main(String[] args) {
    System.out.println(connect());
  }
}
