package wikiapi;

import info.bliki.wiki.dump.WikiArticle;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import wikiapi.processors.PageMeta;

/**
 * 
 * @author cheng88
 *
 */
public class Utils {

  /**
   * Uppercases the string and replaces '_' with ' ' (space) to conform to
   * Wikipedia standards
   * 
   * @param str
   * @return string conforming to Wikipedia standards
   */
  public static String str2wikilink(String str) {
    if (StringUtils.isEmpty(str))
      return str;
    if (Character.isLowerCase(str.codePointAt(0)))
      str = str.substring(0, 1).toUpperCase()
          + (str.length() > 1 ? str.substring(1) : "");
    return StringUtils.replaceChars(str, '_', ' ');
  }

  /**
   * Bounds the number concurrent executing thread to 1/2 of the cores available
   * to the JVM. If more jobs are submitted than the allowed upperbound, the
   * caller thread will be executing the job.
   * 
   * @return a fixed thread pool with bounded job numbers
   */
  public static ThreadPoolExecutor getBoundedThreadPool() {
    int coreCount = Runtime.getRuntime().availableProcessors();
    coreCount = Math.max(coreCount, 80);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(coreCount / 2 + 1, // Core
        coreCount, // Pool Max
        60, TimeUnit.SECONDS, // Thread keep alive time
        new ArrayBlockingQueue<Runnable>(coreCount),// Queue
        new ThreadPoolExecutor.CallerRunsPolicy()// Blocking mechanism
    );
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  /**
   * Bounds the number concurrent executing thread to 1/2 of the cores available
   * to the JVM. If more jobs are submitted than the allowed upperbound, the
   * caller thread will be executing the job.
   * 
   * @return a fixed thread pool with bounded job numbers
   */
  public static ThreadPoolExecutor getBoundedThreadPool(int poolSize) {
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

  // Set of special page prefixes
  private static final Set<String> prefixes = new HashSet<String>(
      Arrays.asList("Media", "Special", "", "Talk", "User", "User_talk",
          "Meta", "Meta_talk", "Image", "Image_talk", "MediaWiki",
          "MediaWiki_talk", "Template", "Template_talk", "Help", "Help_talk",
          "Category", "Category_talk", "File", "File_talk", "Wikipedia",
          "Wikipedia_talk", "Portal", "Portal_talk", "WP", "Project", "CAT",
          "MOS", "Media"));

  /**
   * 
   * @param title
   * @return whether this page is a special page in Wikipedia
   */
  public static boolean isSpecialTitle(String title) {
    if (!StringUtils.isEmpty(title) && title.contains(":")) {
      return prefixes.contains(StringUtils.substringBefore(title, ":"));
    }
    return false;
  }

  /**
   * Checks whether there is actual content in the given page.
   * 
   * @param page
   * @param meta
   * @param ta
   * @return
   */
  public static boolean isNoncontentPage(WikiArticle page, PageMeta meta) {
    return !page.isMain() || page.getText() == null
        || meta.isDisambiguationPage() || meta.isRedirect();
  }

}
