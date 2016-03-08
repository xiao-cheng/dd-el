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

import com.google.common.collect.Lists;

import wikiapi.WikiDumpFilter.Href;
import wikiapi.processors.LinkAnnotationConverter;
import wikiapi.processors.PageMeta;
import wikiapi.processors.PlainTextWikiModel;

/**
 * Parses Wikipedia dump to Wikifier data formats
 * 
 * @author cheng88
 *
 */
public abstract class WikiDumpFilter implements IArticleFilter {

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

    @Override
    public String toString() {
      return "Href [start=" + start + ", end=" + end + ", link=" + link + "]";
    }
    
  }

  private int totalParsed = 0;
  private int prevCount = 0;
  private long prevTime;
  private final ThreadPoolExecutor parsing;
  private Predicate<String> filter = null;
  private boolean printProgress = true;

  /**
   * Multi-threaded parsing with single dump I/O
   */
  public WikiDumpFilter() {
    this(null);
  }

  public WikiDumpFilter(int threadCount) {
    parsing = Utils.getBoundedThreadPool(threadCount);
  }

  public WikiDumpFilter(Predicate<String> filter) {
    parsing = Utils.getBoundedThreadPool();
    this.filter = filter;
  }

  /**
   * In addition to regular parsing, filter hyper links according to the
   * specification
   */
  public WikiDumpFilter(Predicate<String> filter, int threadCount) {
    parsing = Utils.getBoundedThreadPool(threadCount);
    this.filter = filter;
  }

  /**
   * Suppress progress output
   * 
   * @return
   */
  public WikiDumpFilter silence() {
    printProgress = false;
    return this;
  }

  /**
   * 
   * @param filter
   * @return
   */
  public WikiDumpFilter setTitleFilter(Predicate<String> filter) {
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
        if (!StringUtils.isEmpty(text)) {
          // Call back
          processAnnotation(page, new PageMeta(page), text, links);
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
      processAnnotation(page, new PageMeta(page), "", Collections.emptyList());
    }
  }

  /**
   * Waits for all pasring jobs to finish If not called, there might be pages
   * still being parsed
   */
  public void finishUp() {
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
   * @param ta
   *          , null for redirect pages. CorpusId is the Wikipedia article title
   *          and Id is the Wikipedia article id
   * 
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
  public static void parseDump(String file, WikiDumpFilter parser)
      throws UnsupportedEncodingException, FileNotFoundException, IOException,
      SAXException {
    new WikiXMLParser(file, parser).parse();
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
  public static void parseDump(InputStream is, WikiDumpFilter parser)
      throws UnsupportedEncodingException, FileNotFoundException, IOException,
      SAXException {
    new WikiXMLParser(is, parser).parse();
    parser.finishUp();
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

    try {
      System.err.println("Started dump parsing");
      WikiDumpFilter filter = new WikiDumpFilter() {

        @Override
        public void processAnnotation(WikiArticle page, PageMeta meta,
            String text, List<Href> links) {
          System.err.println(StringUtils.abbreviate(text, 100));
          System.err.println(StringUtils.abbreviate(links.toString(), 100));
        }

      };
      parseDump(System.in, filter);
      System.err.println("\nParsing done! Totalling "
          + filter.getParsedPageCount() + " articles.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}