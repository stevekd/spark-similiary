package com.bbtree;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created by chenzhilei on 16/7/11.
 */
public class DateTools {

    public static String format(String str, String origin, String target) {
        if (StringUtils.isBlank(str)) {
            return "";
        }
        str = str.trim();
        if (str.startsWith("[")) {
            str = str.substring(1);
        }
        if (str.endsWith("]")) {
            str = str.substring(0, str.length() - 1);
        }
//        SimpleDateFormat origin = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
//        SimpleDateFormat target = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

        SimpleDateFormat originSf = new SimpleDateFormat(origin, Locale.US);
        SimpleDateFormat targetSf = new SimpleDateFormat(target, Locale.US);
        try {
            return targetSf.format(originSf.parse(str));
        } catch (ParseException e) {
            return "";
        }
    }

    public static void main(String[] args) {
        System.out.println(DateTools.format("11/Jul/2016:11:48:48 +0800", "dd/MMM/yyyy:HH:mm:ss Z", "yyyy-MM-dd HH:mm:ss"));
    }
}
