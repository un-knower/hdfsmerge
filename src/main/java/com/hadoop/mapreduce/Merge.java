package com.hadoop.mapreduce;

import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * Created by zhuxt on 2016/6/22.
 */
public class Merge {
    private static Configuration conf = new Configuration();

    /**
     * 合并文件
     * @param srcPath 要合并的文件所在目录
     * @param dstFile 合并后的文件名，完整路径
     * @param regex   需要合并文件的正则表达式
     * @param delete  合并后是否删除源文件
     */
    public static void Merge(String srcPath, String dstFile,String regex, boolean delete) {
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        try {

            FileSystem fileSystem = FileSystem.get(conf);

            Path source = new Path(srcPath + "/*");

            Path dest = new Path(dstFile);
            //删除目的文件
            fileSystem.delete(dest,false);

            //根据正则表达式过滤文件
            FileStatus[] localStatus = fileSystem.globStatus(source, new RegexAcceptPathFilter(regex));
            //获取目录下所有文件
            Path[] files = FileUtil.stat2Paths(localStatus);
            //输出路径
            Path outPath = new Path(dstFile);
            //打开输出流
            out = fileSystem.create(outPath);
            long start = System.currentTimeMillis();
            for (Path file : files) {
                System.out.println("Starting merge " + file.getName());
                //打开输入流
                in = fileSystem.open(file);
                //复制数据
                IOUtils.copyBytes(in, out, conf, false);
                //关闭输入流
                in.close();
                System.out.println("end merge " + file.getName());
            }
            System.out.println("merge is done, time:" + (System.currentTimeMillis() - start) + "ms" );
            //合并完成后删除文件
            if(delete) {
                for (Path file : files) {
                    fileSystem.delete(file, false);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("merge failed!");
        }finally {
            if (out != null) {
                try {
                    //关闭输出流
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        String srcPath = "/data_center/weibo/year=2016/month=06/";
        String destFile = "/data_center/weibo/year=2016/month=06/2016-06-21.txt";
        String regex = "^2016-06-21.*.txt$";
        Merge(srcPath,destFile,regex,false);
        /*if (args.length < 4) {
            System.out.println("usage: hadoop jar merge.jar Merge srcPath destPath regex isDelete(default false)");
            return;
        }
        boolean delete = false;
        if(args.length == 4){
            delete = Boolean.getBoolean(args[3]);
        }
        Merge(args[0], args[1], args[2], delete);*/
    }
}
