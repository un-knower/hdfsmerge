package com.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

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
        return true;
    }
}
