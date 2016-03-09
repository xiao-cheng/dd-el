package wikiapi;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import info.bliki.wiki.dump.WikiArticle;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import wikiapi.processors.PageMeta;

/**
 * Dumps Wikipedia content into several csv files for importing into databases
 * Writes to working directory
 * @author Xiao Cheng
 *
 */
public class CSVDumper {

  private static Writer getWriter(Path parent, String filename)
      throws IOException {
    Path child = parent.resolve(filename);
    return Files.newBufferedWriter(child, CREATE, WRITE);
  }
  
  /**
   * Converts objects into a single csv line including line break
   * @param fields
   * @return
   */
  private static String csvLine(Object... fields) {
    return Arrays.stream(fields)
        .map(String::valueOf)
        .map(StringEscapeUtils::escapeCsv)
        .collect(Collectors.joining(",")) + "\n";
  }
  
  /**
   * Writes the output to the underlying writer in thread-safe way
   * @param w
   * @param output
   */
  private static void synchronizedWrite(Writer w,String output){
    synchronized(w){
      try {
        w.write(output);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) {

    // Path to the output folder
    Path p = Paths.get(args.length > 0 ? args[0] : System
        .getProperty("user.dir"));
    
    System.err.println("Saving CSV files to " + p);
    try (Writer pageWriter = getWriter(p, "pages.csv");
        Writer linkWriter = getWriter(p, "links.csv");
        Writer redirectWriter = getWriter(p, "redirects.csv");

        WikiDumpParser parser = new WikiDumpParser() {

          @Override
          public void processAnnotation(WikiArticle page, PageMeta meta,
              String text, List<Href> links) {

            String id = page.getId();
            String title = Utils.str2wikilink(page.getTitle());
            String category_str = StringUtils.join(meta.getCategories(), '|');
            
            // Write page dumps
            String pageStr = csvLine(id, title, text, category_str);
            synchronizedWrite(pageWriter, pageStr);

            // Write links
            if (!links.isEmpty()){
              String linkStr = links.stream()
              .map(h -> csvLine(id, h.start, h.end, h.getSurface(text), h.normalizedLink()))
              .collect(Collectors.joining());
              
              synchronizedWrite(linkWriter, linkStr);
            }
            
            // Write redirects
            String redirectTarget = meta.getRedirectedTitle();
            if (redirectTarget != null) {
              String redirectStr = csvLine(id, title, meta.getRedirectedTitle());
              synchronizedWrite(redirectWriter, redirectStr);
            }
            
          }

        };) {
      // Start the parsing process
      parser.parseDump(System.in);
      
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
