package wikiapi;

import static wikiapi.HTMLWikiModel.normalizeTitle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

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
      BiMap<String, Integer> ids) {
    Map<Integer, String> titles = ids.inverse();
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
        if (StringUtils.containsNone(redirect, '#', ':')) {
          System.err.println("[[" + redirect + "]] has no id");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  } );
    return redirects
        .entrySet()
        .parallelStream()
        .filter(
            e -> {
              Integer v = e.getValue();
              Integer end = null;
              Set<Integer> loop = Sets.newHashSet(v);
              while ((end = redirects.getOrDefault(v, null)) != null) {
                if (loop.contains(end)) {
                  System.err.println("Redirect loop detected:"
                      + loop.stream().map(titles::get)
                          .collect(Collectors.joining(",")));
                  // Drop loops from redirects
                  return false;
                }
                e.setValue(end);
                v = end;
                loop.add(v);
              }
              return true;
            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  private static void getLinks(){
    
  }
  
  public static void main(String[] args) {
    try (Connection c = DB.getConnection()) {
      long start = System.currentTimeMillis();
      BiMap<String, Integer> ids = getIds(c);
      Map<Integer, String> titles = ids.inverse();
      Map<Integer, Integer> redirects = getRedirects(c, ids);
      
      
      System.out.println("Article ids size "+ids.size());
      long elpased = (System.currentTimeMillis() - start);
      System.out.println(elpased / 1000 + " seconds");
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(0);
    }
  }
}
