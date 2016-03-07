package wikiapi;

import static wikiapi.HTMLWikiModel.normalizeTitle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.luaj.vm2.ast.Stat.Return;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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

  private static BiMap<String, Integer> getIds(Connection c) {
    String sql = "select id, title from wiki_pages";
    final BiMap<String, Integer> ids = HashBiMap.create();
    query(c, sql, rs -> {
      try {
        int id = rs.getInt(1);
        String title = normalizeTitle(rs.getString(2));
        ids.put(title, id);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    return ids;
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
          // Subsection links are actively ignored
        if (!redirect.contains("#")) {
          System.err.println("[[" + redirect + "]] has no id");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  } );
    return redirects;
  }

  public static void main(String[] args) {
    try (Connection c = DB.getConnection()) {
      long start = System.currentTimeMillis();
      BiMap<String, Integer> ids = getIds(c);
      Map<Integer, String> titles = ids.inverse();
      Map<Integer, Integer> redirects = getRedirects(c, ids);
      // flatten redirects
      redirects.replaceAll((k, v) -> {
        Integer end = null;
        Set<Integer> loop = Sets.newHashSet(v);
        while ((end = redirects.getOrDefault(v, null)) != null) {
          if (loop.contains(end)) {
            System.err.println("Loop detected:"
                + loop.stream().map(titles::get)
                    .collect(Collectors.joining(",")));
            // Return the smallest in the loop for consistency
            return Collections.min(loop);
          }
          v = end;
          loop.add(v);
        }
        return v;
      });
      for (Integer target : redirects.keySet()) {
        if (redirects.containsKey(target)) {
          String nonflat = ids.inverse().get(target);
          System.out.println(nonflat);
        }
      }
      System.out.println(ids.size());
      long elpased = (System.currentTimeMillis() - start);
      System.out.println(elpased / 1000 + " seconds");
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(0);
    }
    System.out.println("Success");
  }

}
