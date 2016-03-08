package wikiapi.processors;

import info.bliki.htmlcleaner.ContentToken;
import info.bliki.htmlcleaner.EndTagToken;
import info.bliki.htmlcleaner.TagNode;
import info.bliki.htmlcleaner.Utils;
import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.model.ImageFormat;
import info.bliki.wiki.tags.HTMLTag;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

public abstract class LinkAnnotationConverter implements ITextConverter{
    
    private boolean fNoLinks;
    private boolean keepSectionTitle;
    
    private static final String TEMPLATE_OPEN = "{{";
    private static final String TEMPLATE_END = "}}";
    
    private static String renderSpecialTags(HTMLTag tag,String href){
        
        // Literal template
        if("{{transl}}".equals(tag.getBodyString())){
            return StringUtils.replaceChars(href, '_', ' ');
        }
        return "";
    }
    
    private static String cleanContent(String content) throws IOException{
        int depth = 0;
        content = Utils.escapeXml(content, true, true, true);
        content = StringEscapeUtils.unescapeHtml4(content);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < content.length();) {
            if (content.startsWith(TEMPLATE_OPEN, i)) {
                depth++;
                i += TEMPLATE_OPEN.length();
                continue;
            }
            if(content.startsWith(TEMPLATE_END,i)) {
                depth--;
                i += TEMPLATE_END.length();
                continue;
            }
            if (depth == 0){
                sb.append(content.charAt(i));
            }
            i++;
        }
        return sb.toString();
    }
    
    public LinkAnnotationConverter(boolean noLinks,boolean keepSectionTitle){
        this.fNoLinks = noLinks;
        this.keepSectionTitle = keepSectionTitle;
    }
    
    public LinkAnnotationConverter(boolean noLinks) {
        this(noLinks,false);
    }

    public LinkAnnotationConverter() {
        this(false);
    }

    @SuppressWarnings("unchecked")
    public void nodesToText(List<? extends Object> nodes, Appendable resultBuffer, IWikiModel model) throws IOException {
        if (nodes != null && !nodes.isEmpty()) {
            try {
                int level = model.incrementRecursionLevel();

                if (level > Configuration.RENDERER_RECURSION_LIMIT) {
                    resultBuffer
                            .append("<span class=\"error\">Error - recursion limit exceeded rendering tags in HTMLConverter#nodesToText().</span>");
                    return;
                }
                for (Object item: nodes) {
                    if (item != null) {
                        if (item instanceof List) {
                            nodesToText((List<? extends Object>) item, resultBuffer, model);
                        } else if (item instanceof ContentToken) {
                            ContentToken contentToken = (ContentToken) item;
                            String content = contentToken.getContent();
                            resultBuffer.append(cleanContent(content));
                        } else if (item instanceof HTMLTag) {
                            HTMLTag tag = (HTMLTag) item;
                            String tagName = tag.getName();
                            // Drop references
                            if("ref".equals(tagName)){
                                continue;
                            }
                            if("a".equals(tagName)){
                                String link = tag.getAttributes().get("href");
                                if (link != null){
                                    CharSequence buffer = ((CharSequence)resultBuffer);
                                    // Make sure tokenized correctly
                                    resultBuffer.append(' ');
                                    int start = buffer.length();
                                    tag.renderHTMLWithoutTag(this, resultBuffer, model);
                                    int end = buffer.length();
                                    if(blank(buffer,start, end)){
                                        
                                        resultBuffer.append(renderSpecialTags(tag,link));
                                        end = buffer.length();
                                        // If fixed
                                        if(!blank(buffer,start, end)){
                                            hasLink(start, end, link);
                                        }
                                        // Special templates are ignored
//                                        else{
//                                            StringBuilder sb = new StringBuilder();
//                                            tag.renderHTML(this, sb, model);
//                                            String s = sb.toString();
//                                            System.out.println("Nothing rendered ?"+s);
//                                        }
                                    }else{
                                        hasLink(start, end, link);
                                    }
                                    resultBuffer.append(' ');
                                    continue;
                                }
                            }
                            tag.renderHTMLWithoutTag(this, resultBuffer, model);
                            
                        } else if (item instanceof TagNode) {
                            TagNode node = (TagNode) item;
                            String tagName = node.getName();
                            if(!keepSectionTitle && "span".equals(tagName))
                                continue;
                            if("a".equals(tagName))
                                resultBuffer.append(node.getBodyString());
                            else
                                System.err.println("Node Type "+tagName+" is not handled.");
//                            
//                            Map<String, Object> map = node.getObjectAttributes();
//                            
//                            if (map == null || map.isEmpty()) {
//                                nodeToHTML(node, resultBuffer, model);
//                            }
                        } else if (item instanceof EndTagToken) {
                            EndTagToken node = (EndTagToken) item;
                            String tagName = node.getName();
                            if("br".equals(tagName)||"hr".equals(tagName))
                                resultBuffer.append("\n\n");
                            else
                                System.err.println("Node Type "+tagName+" is not handled.");
                        }
//                        if(resultBuffer.toString().endsWith("a>")){
//                            System.out.println("app");
//                        }
                    }
                }
            } finally {
                model.decrementRecursionLevel();
            }
        }
    }
    
    private static boolean blank(CharSequence seq,int start,int end){
        if (start >= end || start < 0)
            return true;
        return StringUtils.isWhitespace(seq.subSequence(start, end).toString());
    }

    protected void nodeToHTML(TagNode node, Appendable resultBuffer, IWikiModel model) throws IOException {
        String name = node.getName();
        if (HTMLTag.NEW_LINES) {
            if (name.equals("div") || name.equals("p") || name.equals("table") || name.equals("ul") || name.equals("ol")
                    || name.equals("li") || name.equals("th") || name.equals("tr") || name.equals("td") || name.equals("pre")) {
                resultBuffer.append('\n');
            }
        }
        resultBuffer.append('<');
        resultBuffer.append(name);

        Map<String, String> tagAtttributes = node.getAttributes();

        for (Map.Entry<String, String> currEntry : tagAtttributes.entrySet()) {
            String attName = currEntry.getKey();
            if (attName.length() >= 1 && Character.isLetter(attName.charAt(0))) {
                String attValue = currEntry.getValue();

                resultBuffer.append(" ");
                resultBuffer.append(attName);
                resultBuffer.append("=\"");
                resultBuffer.append(attValue);
                resultBuffer.append("\"");
            }
        }

        List<Object> children = node.getChildren();
        if (children.size() == 0 && !name.equals("a")) {
            resultBuffer.append(" />");
        } else {
            resultBuffer.append('>');
            if (children.size() != 0) {
                nodesToText(children, resultBuffer, model);
            }
            resultBuffer.append("</");
            resultBuffer.append(node.getName());
            resultBuffer.append('>');
        }
    }

    public void imageNodeToText(TagNode imageTagNode, ImageFormat imageFormat, Appendable resultBuffer, IWikiModel model)
            throws IOException {
    }

    public boolean noLinks() {
        return fNoLinks;
    }
    
    public abstract void hasLink(int charStart, int charEnd, String link);

}
