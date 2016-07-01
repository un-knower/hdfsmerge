package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by zhuxt on 2016/6/22.
 */
public class Merge {
    private static Logger log = Logger.getLogger(Merge.class);
    private static Configuration conf = new Configuration();

    /**
     * 合并文件
     * @param srcPath 要合并的文件所在目录
     * @param dstFile 合并后的文件名，完整路径
     * @param regex   需要合并文件的正则表达式
     * @param delete  合并后是否删除源文件
     */
    public static void Merge(String srcPath, String dstFile,String regex, boolean delete) {
        FileSystem fileSystem = null;
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        try {

            fileSystem = FileSystem.get(conf);

            Path source = new Path(srcPath + "/*");
            log.info("source path: " + source.toString());
            Path dest = new Path(dstFile);
            log.info("output file: " + dest.toString());
            log.info("regex expression: " + regex);

            if (delete) {
                log.info("This will delete all source files after merge!");
            }

            //根据正则表达式过滤文件
            FileStatus[] localStatus = fileSystem.globStatus(source, new RegexAcceptPathFilter(regex));
            //获取目录下所有文件
            Path[] files = FileUtil.stat2Paths(localStatus);
            if (files.length == 0) {
                log.info("no matcher files in " + srcPath);
                return;
            }
            //删除目的文件
            if (fileSystem.exists(dest)) {
                fileSystem.delete(dest,false);
                log.info("delete file" + dest.toString());
            }
            //输出路径
            Path outPath = new Path(dstFile);
            //打开输出流
            out = fileSystem.create(outPath);
            long start = System.currentTimeMillis();
            for (Path file : files) {
                log.info("Starting merge " + file.getName());
                //打开输入流
                in = fileSystem.open(file);
                //复制数据
                IOUtils.copyBytes(in, out, conf, false);
                //关闭输入流
                in.close();
                log.info("end merge " + file.getName());
            }
            log.info("merge is done, time:" + (System.currentTimeMillis() - start) + "ms" );
            //合并完成后删除文件
            if(delete) {
                for (Path file : files) {
                    fileSystem.delete(file, false);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.info("merge failed!");
        }finally {
            if (out != null) {
                try {
                    //关闭输出流
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        /*conf.set("fs.defaultFS", "hdfs://10.38.11.59:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/data_center/weibo/year=2016/month=06/2016-06-21.txt");
        System.out.println(path.toString());
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,false);
        }*/

        /*conf.set("fs.defaultFS", "hdfs://10.38.11.59:9000");
        String srcPath = "/data_center/weibo/year=2016/month=06/";
        String destFile = "/data_center/weibo/year=2016/month=06/2016-06-21.txt";
        String regex = "2016-06-21.*.txt";
        Merge(srcPath,destFile,regex,true);*/
        if (args.length < 3) {
            log.info("usage: hadoop jar merge.jar Merge srcPath destPath regex isDeleteSrcFile(default false)");
            return;
        }
        boolean delete = false;
        if(args.length == 4){
            delete = Boolean.parseBoolean(args[3]);
        }
        Merge(args[0], args[1], args[2], delete);
    }
}
