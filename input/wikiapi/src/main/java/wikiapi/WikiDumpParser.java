package wikiapi;

import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.model.WikiModel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.xml.sax.SAXException;

import wikiapi.processors.LinkAnnotationConverter;
import wikiapi.processors.PageMeta;
import wikiapi.processors.PlainTextWikiModel;

/**
 * Parses Wikipedia dump to Wikifier data formats
 * 
 * @author cheng88
 *
 */
public abstract class WikiDumpParser implements IArticleFilter, AutoCloseable {

  public static class Href {
    final int start;
    final int end;
    final String link;

    public Href(String link, int start, int end) {
      super();
      this.start = start;
      this.end = end;
      this.link = link;
    }
    
    /**
     * Extracts the substring in the text according to the offset
     * @param text
     * @return
     */
    public String getSurface(String text){
      return text.substring(start, end);
    }

    @Override
    public String toString() {
      return "Href [start=" + start + ", end=" + end + ", link=" + link + "]";
    }

    public Object normalizedLink() {
      return Utils.str2wikilink(link);
    }

  }

  private int totalParsed = 0;
  private int prevCount = 0;
  private long prevTime;
  private final ThreadPoolExecutor parsing;
  private Predicate<String> filter = null;
  private boolean printProgress = true;
  private static final List<Href> NO_LINKS = Collections.emptyList();

  /**
   * Multi-threaded parsing with single dump I/O
   */
  public WikiDumpParser() {
    this(null);
  }

  public WikiDumpParser(int threadCount) {
    parsing = Utils.getBoundedThreadPool(threadCount);
  }

  public WikiDumpParser(Predicate<String> filter) {
    parsing = Utils.getBoundedThreadPool();
    this.filter = filter;
  }

  /**
   * In addition to regular parsing, filter hyper links according to the
   * specification
   */
  public WikiDumpParser(Predicate<String> filter, int threadCount) {
    parsing = Utils.getBoundedThreadPool(threadCount);
    this.filter = filter;
  }

  /**
   * Suppress progress output
   * 
   * @return
   */
  public WikiDumpParser silence() {
    printProgress = false;
    return this;
  }

  /**
   * 
   * @param filter
   * @return
   */
  public WikiDumpParser setTitleFilter(Predicate<String> filter) {
    this.filter = filter;
    return this;
  }

  public void process(final WikiArticle page, Siteinfo siteinfo)
      throws SAXException {
    if (printProgress && totalParsed == 0) {
      prevTime = System.currentTimeMillis();
    }
    if (page.isMain() && !StringUtils.isEmpty(page.getText())
        && !Utils.isSpecialTitle(page.getTitle())) {
      WikiModel wikiModel = new PlainTextWikiModel(siteinfo, filter);
      // Concurrent callback
      parsing.execute(() -> {
        wikiModel.setUp();
        final List<Href> links = new ArrayList<Href>();
        LinkAnnotationConverter renderer = new LinkAnnotationConverter() {
          public void hasLink(int charStart, int charEnd, String href) {
            links.add(new Href(href, charStart, charEnd));
          }
        };
        String text = wikiModel.render(renderer, page.getText());
        PageMeta meta = new PageMeta(page);
        if (StringUtils.isEmpty(text)) {
          processAnnotation(page, meta, "", NO_LINKS);
        } else {
          // Call back
          processAnnotation(page, meta, text, links);
        }
      });
      if (printProgress && ++totalParsed % 500 == 0) {
        double timeLapsed = (System.currentTimeMillis() - prevTime) / 1000.;
        prevTime = System.currentTimeMillis();
        double pagesPerSecond = (totalParsed - prevCount) / timeLapsed;
        prevCount = totalParsed;
        System.err
            .printf("%d pages at %.2f/sec\n", totalParsed, pagesPerSecond);
        System.err.printf("Active threads %d/%d\n", parsing.getActiveCount(),
            parsing.getPoolSize());
      }
    } else {
      processAnnotation(page, new PageMeta(page), "", NO_LINKS);
    }
  }

  /**
   * Waits for all parsing jobs to finish If not called, there might be pages
   * still being parsed
   */
  public void close() {
    parsing.shutdown();
    try {
      parsing.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * The parsed results for each page, redirect pages get null TextAnnotation
   * Note that this method is called asynchronously for performance reasons,
   * thus storing parsed information requires external synchronization.
   * 
   * @param page
   * @param meta Including information on redirects, categories etc.
   * @param text Plain text rendering of the page
   * @param links Intrasite hyperlinks marked by character offsets
   */
  public abstract void processAnnotation(WikiArticle page, PageMeta meta,
      String text, List<Href> links);

  /**
   * @return the number of parsing jobs submitted to the parser
   */
  public int getParsedPageCount() {
    return totalParsed;
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
  public void parseDump(String file)
      throws UnsupportedEncodingException, FileNotFoundException, IOException,
      SAXException {
    new WikiXMLParser(file, this).parse();
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
  public void parseDump(InputStream is)
      throws UnsupportedEncodingException, FileNotFoundException, IOException,
      SAXException {
    new WikiXMLParser(is, this).parse();
  }

  /**
   * Prints simple information while parsing the dump
   * 
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    if (args.length > 0) {
      System.err.println("Usage: bzcat dump.xml.bz2 | WikiDumpFilter");
      System.exit(-1);
    }

    try (WikiDumpParser parser = new WikiDumpParser() {

      @Override
      public void processAnnotation(WikiArticle page, PageMeta meta,
          String text, List<Href> links) {
        System.err.println("#TITLE--"+page.getTitle());
        if(meta.isRedirect()){
          System.err.println("#REDIRECT--"+meta.getRedirectedTitle());
        }else{
          System.err.println(StringUtils.abbreviate(text.trim(), 100));
          System.err.println(StringUtils.abbreviate(links.toString(), 100));
          links.stream().limit(2).forEach(h->{
            System.err.println("#SURFACE--"+text.substring(h.start, h.end));
          });
          meta.getCategories().stream().limit(2).forEach(System.err::println);
        }
      }

    }) {
      System.err.println("Started dump parsing");
      boolean debug = true;
      if (debug) {
        String file = "/Users/xiaocheng/Downloads/enwiki-sample-pages-articles.xml.bz2";
        parser.parseDump(file);
      }
      parser.parseDump(System.in);
      System.err.println("\nParsing done! Totalling "
          + parser.getParsedPageCount() + " articles.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}