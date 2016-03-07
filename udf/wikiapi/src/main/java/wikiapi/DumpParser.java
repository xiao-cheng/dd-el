package wikiapi;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.model.WikiModel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import ch.qos.logback.classic.Level;

//import com.sun.org.apache.xml.internal.resolver.helpers.FileURL;

/**
 * Parses Wikipedia dump to html format by printing
 * to STDOUT in CSV format
 * 
 * @author cheng88
 *
 */
public class DumpParser implements IArticleFilter {

  private static boolean debug = false;
  private int totalParsed = 0;
  private int prevCount = 0;
  private long prevTime;
  private final ThreadPoolExecutor parsing;
  private boolean printProgress = true;
  
  // More threads than this would not help
  private static final int MAX_THREADS = 80;

  /**
   * Multi-threaded parsing with single dump I/O
   */
  private DumpParser() {
    // Defaults to the number of real cores
    this(Math.min(MAX_THREADS, Runtime.getRuntime().availableProcessors() / 2));
  }

  private DumpParser(int threadCount) {
    parsing = getBoundedThreadPool(threadCount);
  }

  /**
   * Suppress progress output
   * 
   * @return
   */
  public DumpParser silence() {
    printProgress = false;
    return this;
  }

  /**
   * @override
   */
  public void process(final WikiArticle page, Siteinfo siteinfo) {
    if (printProgress && totalParsed == 0) {
      prevTime = System.currentTimeMillis();
    }
    boolean isContentPage = page.isMain() || page.isCategory(); 
    if (isContentPage && !StringUtils.isEmpty(page.getText()))
      parsing.execute(() -> {
        WikiModel model = new HTMLWikiModel();
        String id = page.getId();
        String title = page.getTitle();
        String mediawiki = page.getText();
        try {
          String html = model.render(mediawiki);
          // Prints 4 column rows
          String output = Arrays.asList(id, title, html, mediawiki)
              .stream()
              .map(StringEscapeUtils::escapeCsv)
              .collect(Collectors.joining(","));
          synchronized (parsing) {
            System.out.println(output);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });

    if (printProgress && ++totalParsed % 500 == 0) {
      double timeLapsed = (System.currentTimeMillis() - prevTime) / 1000.;
      prevTime = System.currentTimeMillis();
      double pagesPerSecond = (totalParsed - prevCount) / timeLapsed;
      prevCount = totalParsed;
      System.err.printf("%d pages at %.2f/sec\n", totalParsed, pagesPerSecond);
      System.err.printf("Active threads %d/%d\n", parsing.getActiveCount(),
          parsing.getPoolSize());
    }
  }

  /**
   * Terminates the thread pool wait for them to finish
   */
  private void finishUp() {
    parsing.shutdown();
    try {
      parsing.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parses the given Wikipedia XML dump file. User needs to instantiate the
   * parser for call backs
   * 
   * @param file
   * @param parser
   * @throws UnsupportedEncodingException
   * @throws FileNotFoundException
   * @throws IOException
   * @throws SAXException
   */
  public static void parseDumpWith(DumpParser parser)
      throws UnsupportedEncodingException, FileNotFoundException, IOException,
      SAXException {
    new WikiXMLParser(System.in, parser).parse();
    parser.finishUp();
  }

  /**
   * Parses the given Wikipedia XML dump stream. User needs to instantiate the
   * parser for call backs
   * 
   * @param file
   * @param parser
   * @throws UnsupportedEncodingException
   * @throws FileNotFoundException
   * @throws IOException
   * @throws SAXException
   */
  public static void parseDumpWith(InputStream is, DumpParser parser)
      throws UnsupportedEncodingException, FileNotFoundException, IOException,
      SAXException {
    new WikiXMLParser(is, parser).parse();
    parser.finishUp();
  }

  /**
   * Bounds the number concurrent executing thread to 1/2 of the cores available
   * to the JVM. If more jobs are submitted than the allowed upperbound, the
   * caller thread will be executing the job.
   * 
   * @return a fixed thread pool with bounded job numbers
   */
  private static ThreadPoolExecutor getBoundedThreadPool(int poolSize) {
    poolSize = Math.max(1, poolSize - 1);
    poolSize = Math.min(poolSize, Runtime.getRuntime().availableProcessors());
    ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, // Core count
        poolSize, // Pool Max
        60, TimeUnit.SECONDS, // Thread keep alive time
        new ArrayBlockingQueue<Runnable>(poolSize),// Queue
        new ThreadPoolExecutor.CallerRunsPolicy()// Blocking mechanism
    );
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }
  
  private static void turnOffLogback(){
    Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (rootLogger instanceof ch.qos.logback.classic.Logger) {
      ((ch.qos.logback.classic.Logger) rootLogger).setLevel(Level.OFF);
    }
  }

  /**
   * Prints simple information while parsing the dump
   * 
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    if (args.length > 0) {
      System.err.println("Usage: bzcat latest.xml.bz2 | java ... Parse");
      System.exit(-1);
    }

    // Turn off logging since it only reports when chart/diagram templates
    // can't be drawn in bliki and messes with stdout pipe
    turnOffLogback();

    try {
      System.err.println("Started dump parsing");
      DumpParser parser = new DumpParser();
      if (debug) {
        BZip2CompressorInputStream bi = new BZip2CompressorInputStream(
            new FileInputStream(new File(
                "/Users/xiaocheng/Downloads/enwiki-sample-pages-articles.xml.bz2")));
        parseDumpWith(bi, parser);
      } else {
        parseDumpWith(parser);
      }
      System.err.printf("\nParsing done! Totalling %d articles.\n", parser.totalParsed);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}