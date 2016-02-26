package edu.stanford.cs.hazy.wikiapi;

import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.IOException;

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
