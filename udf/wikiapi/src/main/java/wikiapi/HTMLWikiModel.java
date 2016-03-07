package wikiapi;

import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;

/**
 * Removed the default that enables template parsing
 * 
 * @author Xiao Cheng
 *
 */
public class HTMLWikiModel extends WikiModel {
  
  private static final boolean PARSE_TEMPLATES = true;

  public HTMLWikiModel() {
    super("${image}", "${title}");
  }
  
  public static String normalizeTitle(String title){
    return StringUtils.capitalize(title.trim()).replace('_', ' ');
  }

  @Override
  public String render(ITextConverter converter, String rawWikiText,
      boolean templateTopic) throws IOException {
    initialize();
    if (rawWikiText == null) {
      return "";
    }
    StringBuilder buf = new StringBuilder(rawWikiText.length()
        + rawWikiText.length() / 10);

    render(converter, rawWikiText, buf, templateTopic, PARSE_TEMPLATES);

    return buf.toString();
  }

}
