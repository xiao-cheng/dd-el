package wikiapi.processors;

import info.bliki.htmlcleaner.TagNode;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.filter.WikipediaParser;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.ImageFormat;
import info.bliki.wiki.model.WikiModel;
import info.bliki.wiki.namespaces.INamespace;
import info.bliki.wiki.namespaces.Namespace;
import info.bliki.wiki.tags.WPATag;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;

/**
 * Drops all images and templates, preserves links
 * 
 * @author cheng88
 *
 */
public class PlainTextWikiModel extends WikiModel {

  private Siteinfo fSiteinfo;

  // By default we filter out special page links
  private Predicate<String> filter = new Predicate<String>() {
    public boolean test(String title) {
      if (title.contains(":")) {
        String prefix = StringUtils.substringBefore(title, ":");
        return !isNamespace(prefix);
      }
      return true;
    }
  };

  static {
    TagNode.addAllowedAttribute("style");
  }

  public PlainTextWikiModel(Siteinfo siteInfo) {
    this(siteInfo, "${image}", "${title}");
  }

  public PlainTextWikiModel(Siteinfo siteInfo, Predicate<String> filter) {
    this(siteInfo);
    if (filter != null)
      this.filter = filter;
  }

  /**
   * 
   * 
   * @param wikiDB
   *          a wiki database to retrieve already cached templates
   * @param imageBaseURL
   *          a url string which must contains a &quot;${image}&quot; variable
   *          which will be replaced by the image name, to create links to
   *          images.
   * @param linkBaseURL
   *          a url string which must contains a &quot;${title}&quot; variable
   *          which will be replaced by the topic title, to create links to
   *          other wiki topics.
   * @param imageDirectoryName
   *          a directory for storing downloaded Wikipedia images. The directory
   *          must already exist.
   */
  public PlainTextWikiModel(Siteinfo siteinfo, String imageBaseURL,
      String linkBaseURL) {
    this(siteinfo, Locale.ENGLISH, imageBaseURL, linkBaseURL);
  }

  /**
   * 
   * @param wikiDB
   *          a wiki database to retrieve already cached templates
   * @param locale
   *          a locale for loading language specific resources
   * @param imageBaseURL
   *          a url string which must contains a &quot;${image}&quot; variable
   *          which will be replaced by the image name, to create links to
   *          images.
   * @param linkBaseURL
   *          a url string which must contains a &quot;${title}&quot; variable
   *          which will be replaced by the topic title, to create links to
   *          other wiki topics.
   * @param imageDirectoryName
   *          a directory for storing downloaded Wikipedia images. The directory
   *          must already exist.
   */
  public PlainTextWikiModel(Siteinfo siteinfo, Locale locale,
      String imageBaseURL, String linkBaseURL) {
    super(Configuration.DEFAULT_CONFIGURATION, locale, imageBaseURL,
        linkBaseURL);
    fSiteinfo = siteinfo;
  }

  /**
   * Get the raw wiki text for the given namespace and article name. This model
   * implementation uses a Derby database to cache downloaded wiki template
   * texts.
   * 
   * @param namespace
   *          the namespace of this article
   * @param templateName
   *          the name of the template
   * @param templateParameters
   *          if the namespace is the <b>Template</b> namespace, the current
   *          template parameters are stored as <code>String</code>s in this map
   * 
   * @return <code>null</code> if no content was found
   * 
   * @see info.bliki.api.User#queryContent(String[])
   */
  @Override
  public String getRawWikiContent(String namespace, String articleName,
      Map<String, String> templateParameters) {
    String result = super.getRawWikiContent(namespace, articleName,
        templateParameters);
    if (result != null) {
      // found magic word template
      return result;
    }
    return null;
  }

  public String getRedirectedWikiContent(String rawWikitext,
      Map<String, String> templateParameters) {
    if (rawWikitext.length() < 9) {
      // less than "#REDIRECT" string
      return rawWikitext;
    }
    String redirectedLink = WikipediaParser.parseRedirect(rawWikitext, this);
    if (redirectedLink != null) {
      String redirNamespace = "";
      String redirArticle = redirectedLink;
      int index = redirectedLink.indexOf(":");
      if (index > 0) {
        redirNamespace = redirectedLink.substring(0, index);
        if (isNamespace(redirNamespace)) {
          redirArticle = redirectedLink.substring(index + 1);
        } else {
          redirNamespace = "";
        }
      }
      try {
        int level = incrementRecursionLevel();
        if (level > Configuration.PARSER_RECURSION_LIMIT) {
          return "Error - getting content of redirected link: "
              + redirNamespace + ":" + redirArticle;
        }
        return getRawWikiContent(redirNamespace, redirArticle,
            templateParameters);
      } finally {
        decrementRecursionLevel();
      }
    }
    return rawWikitext;
  }

  /**
   * Ignores all image links
   */
  public void appendInternalImageLink(String hrefImageLink,
      String srcImageLink, ImageFormat imageFormat) {
  }

  public void appendInternalLink(String topic, String hashSection,
      String topicDescription, String cssClass, boolean parseRecursive) {

    String href = null;
    try {
      href = URLDecoder.decode(encodeTitleToUrl(topic, true), "UTF-8");
    } catch (UnsupportedEncodingException e) {
    }
    WPATag aTagNode = new WPATag();
    if (filter.test(href)) {
      aTagNode.addAttribute("href", href, false);
    }
    pushNode(aTagNode);
    WikipediaParser.parseRecursive(topicDescription.trim(), this, false, true);
    popNode();
  }

  public void parseInternalImageLink(String imageNamespace, String rawImageLink) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see info.bliki.wiki.model.AbstractWikiModel#getCategoryNamespace()
   */
  @Override
  public String getCategoryNamespace() {
    return fSiteinfo.getNamespace(INamespace.CATEGORY_NAMESPACE_KEY);
  }

  /*
   * (non-Javadoc)
   * 
   * @see info.bliki.wiki.model.AbstractWikiModel#getImageNamespace()
   */
  @Override
  public String getImageNamespace() {
    return fSiteinfo.getNamespace(INamespace.FILE_NAMESPACE_KEY);
  }

  /*
   * (non-Javadoc)
   * 
   * @see info.bliki.wiki.model.AbstractWikiModel#getTemplateNamespace()
   */
  @Override
  public String getTemplateNamespace() {
    return fSiteinfo.getNamespace(INamespace.TEMPLATE_NAMESPACE_KEY);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * info.bliki.wiki.model.AbstractWikiModel#isCategoryNamespace(java.lang.String
   * )
   */
  @Override
  public boolean isCategoryNamespace(String namespace) {
    return INamespace.CATEGORY_NAMESPACE_KEY.equals(fSiteinfo
        .getIntegerNamespace(namespace));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * info.bliki.wiki.model.AbstractWikiModel#isImageNamespace(java.lang.String)
   */
  @Override
  public boolean isImageNamespace(String namespace) {
    return INamespace.FILE_NAMESPACE_KEY.equals(fSiteinfo
        .getIntegerNamespace(namespace));
  }

  /*
   * (non-Javadoc)
   * 
   * @see info.bliki.wiki.model.AbstractWikiModel#isNamespace(java.lang.String)
   */
  @Override
  public boolean isNamespace(String namespace) {
    return ns.NAMESPACE_MAP.containsKey(namespace)
        || ns.TALKSPACE_MAP.containsKey(namespace);
  }

  private static final Namespace ns = new Namespace();

  /*
   * (non-Javadoc)
   * 
   * @see
   * info.bliki.wiki.model.AbstractWikiModel#isTemplateNamespace(java.lang.String
   * )
   */
  @Override
  public boolean isTemplateNamespace(String namespace) {
    return INamespace.TEMPLATE_NAMESPACE_KEY.equals(fSiteinfo
        .getIntegerNamespace(namespace));
  }

}
