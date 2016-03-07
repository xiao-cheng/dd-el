package edu.stanford.cs.hazy.wikiapi;

import static edu.stanford.cs.hazy.wikiapi.HTMLWikiModel.normalizeTitle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

/**
 * Caches frequently accessed data in hashmap and computes the relevant
 * statistics of entity linking
 * 
 * @author Xiao Cheng
 *
 */
public class CollectStats {
  private static final boolean debug = false;

  private static void query(Connection c, String sql, Consumer<ResultSet> rs) {
    if (debug) {
      sql += " limit 5";
    }
    try (PreparedStatement stmt = c.prepareStatement(sql);
        ResultSet results = stmt.executeQuery()) {
      while (results.next()) {
        rs.accept(results);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static BiMap<String, Integer> getIdCache(Connection c) {
    String sql = "select id, title from wiki_pages";
    final BiMap<String, Integer> cache = HashBiMap.create();
    query(c, sql, rs -> {
      try {
        int id = rs.getInt(1);
        String title = normalizeTitle(rs.getString(2));
        cache.put(title, id);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    return cache;
  }

  private static Map<Integer, Integer> getRedirects(Connection c,
      Map<String, Integer> ids) {

    String sql = "select id, "
        + "substring(mediawiki from '\\[\\[([^\\]]+)\\]\\]') "
        + "from wiki_pages where html is null and lower(mediawiki) "
        + "like '#redirect%'";

    Map<Integer, Integer> redirects = Maps.newHashMap();
    query(c, sql, rs -> {
      try {
        int id = rs.getInt(1);
        String redirect = normalizeTitle(rs.getString(2));
        int redirectId = ids.getOrDefault(redirect, -1);
        if (redirectId >= 0) {
          redirects.put(id, redirectId);
        } else {
          System.err.println("[[" + redirect + "]] has no id");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    return redirects;
  }

  public static void main(String[] args) {
    try (Connection c = DB.getConnection()) {
      long start = System.currentTimeMillis();
      BiMap<String, Integer> ids = getIdCache(c);
      Map<Integer, Integer> redirects = getRedirects(c, ids);
      for(int target:redirects.values()){
        if (redirects.containsKey(target)){
          String nonflat = ids.inverse().get(target);
          System.out.println(nonflat);
        }
      }
      System.out.println(ids.size());
      System.out.println((System.currentTimeMillis() - start) / 1000 + " seconds");
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(0);
    }
    System.out.println("Success");
  }

}
