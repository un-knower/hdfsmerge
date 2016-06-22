package com.hadoop.mapreduce;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhuxt on 2016/6/22.
 */
public class RegexAcceptPathFilter implements PathFilter {
    private final String regex;

    public RegexAcceptPathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(path.getName());
        if (matcher.find()) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        String p = "/data_center/weibo/year=2016/month=06/2016-06-21.txt";
        Path path = new Path(p);
        RegexAcceptPathFilter r = new RegexAcceptPathFilter("2016-06-21..*.txt");
        System.out.println(r.accept(path));
    }
}
