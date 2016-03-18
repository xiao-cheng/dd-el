package wikiapi;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringEscapeUtils;

import info.bliki.wiki.dump.WikiArticle;
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
        .collect(Collectors.joining(",")) + "\n";
  }
  
  private static String csvIntArr(IntStream st) {
    return st.mapToObj(String::valueOf)
        .collect(Collectors.joining(",", "\"{", "}\""));
  }
  
  private static String csvStrArr(Stream<String> st) {
    String escaped = st.map(s->"\""+s.replace("\\","\\\\").replace("\"", "\\\"")+"\"")
        .collect(Collectors.joining(",", "{", "}"));
    return escaped;
  }
  
  private static List<FileWriter> chunkedWriters(String filenameFormat, int chunks){
    return IntStream.range(0, chunks)
    .boxed()
    .map(i-> {
      try {
        return new FileWriter(String.format(filenameFormat, i));
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(0);
      }
      return null;
      })
    .collect(Collectors.toList());
  }
  
  private static void write(List<FileWriter> writers,int jobId,String output){
    FileWriter writer = writers.get(jobId%writers.size());
    try {
      synchronized(writer){
        writer.write(output);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  private static void closeWriters(List<FileWriter> writers) throws IOException{
    for(FileWriter w:writers){
      w.close();
    }
  }
  
  private static String linkCsv(String id, String text, Href h){
    return csvLine(id, h.start, h.end, h.getSurface(text), h.normalizedLink());
  }
  
  public static void main(String[] args) {
    // Path to the output folder
    new File("chunks/").mkdirs();
    int chunks = 10;
    
    List<FileWriter> pageWriters = chunkedWriters("chunks/page%d.csv",chunks);
    List<FileWriter> linkWriters = chunkedWriters("chunks/link%d.csv",chunks);
    List<FileWriter> redirectWriters = chunkedWriters("chunks/redirect%d.csv",chunks);
    
    try {
      WikiDumpParser parser = new WikiDumpParser() {
        @Override
        public void processAnnotation(WikiArticle page, PageMeta meta,
            String plain, List<Href> links,int jobId) {
          
          String title = Utils.str2wikilink(page.getTitle());
          // Write redirects
          String redirectTarget = meta.getRedirectedTitle();
          if (redirectTarget != null) {
            String redirectStr = csvLine(title, meta.getRedirectedTitle());
            write(redirectWriters,jobId,redirectStr);
            return;
          }

          // Fields to to join in CSV
          String id = page.getId();
          Boolean disamb = meta.isDisambiguationPage();
          // Array strings
          String categoryStr = csvStrArr(meta.getCategories().stream());
          
          // Write page dumps
          String pageStr = csvLine(id, title, plain, disamb, categoryStr);
          write(pageWriters,jobId,pageStr);
          
          if (!links.isEmpty()){
            String linkStr = links.stream()
                .map(h -> linkCsv(id, plain, h))
                .collect(Collectors.joining());
            
            write(linkWriters, jobId, linkStr);
          }
          
        }
      };
      // Start the parsing process
      InputStream in = System.in;//CSVDumper.class.getResourceAsStream("test.xml");
      parser.parseDump(in);
      closeWriters(pageWriters);
      closeWriters(linkWriters);
      closeWriters(redirectWriters);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
