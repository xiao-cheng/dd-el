package wikiapi;

import info.bliki.wiki.dump.WikiArticle;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringEscapeUtils;

import wikiapi.WikiDumpParser.Href;
import wikiapi.processors.PageMeta;

/**
 * Dumps Wikipedia content into several csv files for importing into databases
 * Writes to working directory
 * @author Xiao Cheng
 *
 */
public class CSVDumper {
  
  /**
   * Converts objects into a single csv line including line break
   * @param fields
   * @return
   */
  private static String csvLine(Object... fields) {
    return Arrays.stream(fields)
        .map(String::valueOf)
        .map(StringEscapeUtils::escapeCsv)
        .collect(Collectors.joining(","));
  }
  
  private static <T> String csvArr(Stream<T> st) {
    return st.map(String::valueOf)
        .map(StringEscapeUtils::escapeCsv)
        .collect(Collectors.joining(",", "{", "}"));
  }
  
  private static String linkStr(List<Href> links){
    return csvArr(links.stream().map(l->l.start)) + ","
        +csvArr(links.stream().map(l->l.end)) + ","
        +csvArr(links.stream().map(l->l.link));
  }
  
  private static class Page{
    String pageStr;//CSV line of fixed content
    List<Href> links;
    public Page(String pageStr, List<Href> links) {
      super();
      this.pageStr = pageStr;
      this.links = links;
    }
    
  }

  public static void main(String[] args) {
    
    // Path to the output folder
    Object writeLock = new Object();
    try (WikiDumpParser parser = new WikiDumpParser(1) {
      @Override
      public void processAnnotation(WikiArticle page, PageMeta meta,
          String plain, List<Href> links) {

        // Fields to to join in CSV
        String id = page.getId();
        String title = Utils.str2wikilink(page.getTitle());
        Boolean disamb = meta.isDisambiguationPage();
        String redirectTarget = meta.getRedirectedTitle();
        
        // Array strings
        String linkArrayStr = linkStr(links);
        String categoryStr = csvArr(meta.getCategories().stream());
        
        // Write page dumps
        String pageStr = csvLine(
            id, 
            title, 
            plain,
            disamb,
            redirectTarget)
            + "," + 
            linkArrayStr
            + "," +
            categoryStr;
        synchronized (writeLock) {
          System.out.println(pageStr);
        }
      }
    };) {
      // Start the parsing process
      InputStream in = System.in;//CSVDumper.class.getResourceAsStream("test.xml");
      parser.parseDump(in);

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
