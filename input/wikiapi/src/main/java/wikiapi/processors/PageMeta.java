package wikiapi.processors;

import info.bliki.wiki.dump.WikiArticle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import wikiapi.Utils;

/**
 * Copied and modified from info.bliki.wiki.dump.WikiPatternMatcher
 * Hosts a number of standalone utilities to get meta information about a Wikipedia page.
 * Note that this is only guaranteed to work for the current Wikipedia
 * markup (2014.1)
 * @author Delip Rao modified by Axel Kramer; modified by cheng88
 *
 */
public class PageMeta {
    
    private String wikiText = "";
    private List<String> pageCats = Collections.emptyList();
    private List<String> pageLinks = Collections.emptyList();
    private String redirectString = null;
    private Boolean redirect = null;
    private boolean stub = false;
    private boolean disambiguation = false;

    final static Pattern STUB_PATTERN = Pattern.compile("-stub}}",Pattern.LITERAL);
    public static String[] disambiguationTemplates = {"Disambig","Disambiguation","Dab","DAB","Disamb"};
    private static String disPattern = "";
    static{
        String[] lowercased = new String[disambiguationTemplates.length*2];
        int i = 0;
        for(String name:disambiguationTemplates){
            lowercased[i] = name;
            lowercased[i+1] = WordUtils.uncapitalize(name);
            i += 2;
        }
        for(String name:lowercased){
            disPattern+= ("|(\\{\\{"+name+"(\\||\\}))");
        }
        disPattern = disPattern.substring(1);
    }
    
    final static Pattern DISAMB_TEMPLATE_PATTERN = Pattern.compile(disPattern);
    final static Pattern CATEGORY_PATTERN = Pattern.compile("\\[\\[Category:(.*?)\\]\\]", Pattern.MULTILINE);
    final static Pattern LINKS_PATTERN = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE);
    final static String redirectPrefix = "#REDIRECT";


    public PageMeta(WikiArticle article) {
        if (article == null)
            return;
        wikiText = article.getText();
        if (wikiText == null) {
        	wikiText = "";
            redirect = false;
            return;
        }
        if(isRedirect()){
            redirectString = StringUtils.substringBetween(wikiText, "[[", "]]");
            if(StringUtils.isEmpty(redirectString))
                redirect = false;
        }
        stub = STUB_PATTERN.matcher(wikiText).find();
        
        disambiguation = article.getTitle().endsWith("(disambiguation)") 
                || DISAMB_TEMPLATE_PATTERN.matcher(wikiText).find();
    }

    public boolean isRedirect() {
        if (redirect == null) {
            if(wikiText.length()<redirectPrefix.length()){
                redirect = false;
            }else{
                String prefix = wikiText.substring(0, redirectPrefix.length());
                redirect = redirectPrefix.equals(prefix.toUpperCase());
            }
        }
        return redirect;
    }

    public boolean isStub() {
        return stub;
    }

    public String getRedirectText() {
        return redirectString;
    }
    
    public String getRedirectedTitle() {
        if (isRedirect())
            return Utils.str2wikilink(getRedirectText());
        return null;
    }

    public String getText() {
        return wikiText;
    }

    public List<String> getCategories() {
        if (pageCats == null)
            parseCategories();
        return pageCats;
    }

    public List<String> getLinks() {
        if (pageLinks == null)
            parseLinks();
        return pageLinks;
    }

    private void parseCategories() {
        pageCats = new ArrayList<String>();
		if (StringUtils.isEmpty(wikiText))
			return;
        Matcher matcher = CATEGORY_PATTERN.matcher(wikiText);
        while (matcher.find()) {
            String[] temp = StringUtils.split(matcher.group(1),'|');
            if (temp.length > 0)
                pageCats.add(temp[0]);
        }
    }

    private void parseLinks() {
        pageLinks = new ArrayList<String>();
        Matcher matcher = LINKS_PATTERN.matcher(wikiText);
        while (matcher.find()) {
            String[] temp = StringUtils.split(matcher.group(1),'|');
            if (temp == null || temp.length == 0)
                continue;
            String link = temp[0];
            if (link.contains(":") == false) {
                pageLinks.add(link);
            }
        }
    }

    /**
     * Strip wiki formatting characters from the given wiki text.
     * Discouraged as there is no cached regex
     * @return
     */
    public String getPlainText() {
        String text = wikiText.replaceAll("&gt;", ">");
        text = text.replaceAll("&lt;", "<");
        text = text.replaceAll("<ref>.*?</ref>", " ");
        text = text.replaceAll("</?.*?>", " ");
        text = text.replaceAll("\\{\\{.*?\\}\\}", " ");
        text = text.replaceAll("\\[\\[.*?:.*?\\]\\]", " ");
        text = text.replaceAll("\\[\\[(.*?)\\]\\]", "$1");
        text = text.replaceAll("\\s(.*?)\\|(\\w+\\s)", " $2");
        text = text.replaceAll("\\[.*?\\]", " ");
        text = text.replaceAll("\\'+", "");
        return text;
    }

    public boolean isDisambiguationPage() {
        return disambiguation;
    }

    public String getTranslatedTitle(String languageCode) {
        Pattern translatePattern = Pattern.compile("^\\[\\[" + languageCode + ":(.*?)\\]\\]$", Pattern.MULTILINE);
        Matcher matcher = translatePattern.matcher(wikiText);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

}
