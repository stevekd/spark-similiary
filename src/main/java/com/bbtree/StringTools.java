package com.bbtree;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chenzhilei on 16/6/19.
 */
public class StringTools {
    public static String unicodeToString(String str) {
        Pattern pattern = Pattern.compile("(\\\\x)(\\p{XDigit}{2})");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            String group_0 = matcher.group(0);
            String group_2 = matcher.group(2);
            ch = (char) Integer.parseInt(group_2, 16);
            str = str.replace(group_0, ch + "");
        }
        pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
        matcher = pattern.matcher(str);
        while (matcher.find()) {
            String group_0 = matcher.group(0);
            String group_2 = matcher.group(2);
            ch = (char) Integer.parseInt(group_2, 16);
            str = str.replace(group_0, ch + "");
        }
        return str;
    }


    public static String escape(String str) {
//        Pattern pattern = Pattern.compile("(\\\\x)(\\p{XDigit}{2})");
        Pattern pattern = Pattern.compile("(\\\\x)([0-9a-zA-Z]{2})");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            String group_0 = matcher.group(0);
            String group_2 = matcher.group(2);
            System.out.println(group_2);
            ch = (char) Integer.parseInt(group_2, 16);
            str = str.replace(group_0, ch + "");
        }
        return str;
    }

    public static JsonNode parse_json(String json) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode;
        try {
            rootNode = mapper.readTree(json);
        } catch (IOException e) {
            return null;
        }
        return rootNode;
    }


    public static String urlDCode(String str) {
        String decode_body = "";
        try {
            decode_body = URLDecoder.decode(str, "UTF-8");

        } catch (Exception e) {
            return "";
        }
        return decode_body;
    }

}
