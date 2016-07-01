package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

/**
 * Created by zhuxt on 2016/6/30.
 */
public class MRMerge extends Configured implements Tool {

    private static String OUTPUTFILE = "2016-06-21.txt";
    private static String INPUTPATH = "/tmp/test";
    private static String OUTPUTPATH = "/tmp/output/merge";

    @Override
    public int run(String[] strings) throws Exception {
        OUTPUTFILE = strings[3];
        INPUTPATH = strings[0];
        OUTPUTPATH = strings[1];
        FileSystem fileSystem = FileSystem.get(getConf());
        if (fileSystem.exists(new Path(OUTPUTPATH))) {
            fileSystem.delete(new Path(OUTPUTPATH), true);
        }
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MergeMap.class);
        job.setReducerClass(MyReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(MyFileOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job,MyFileOutputFormat.class);
        //根据正则表达式过滤文件
        FileStatus[] localStatus = fileSystem.globStatus(new Path(strings[0] + "/*"), new RegexAcceptPathFilter(strings[2]));
        //获取目录下所有文件
        Path[] files = FileUtil.stat2Paths(localStatus);
        for (Path path : files) {
            FileInputFormat.addInputPath(job, path);
        }
//        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new MRMerge(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public static class MergeMap extends Mapper<LongWritable, Text, Text,
            Text> {
        static enum CountersEnum { INPUT_WORDS }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            context.write(new Text(line),new Text(""));
            Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.name());
            counter.increment(1);
        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {

        public void reducer(Text key, Text value, Context context) {
            try {
                context.write(key, new Text(""));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static class MyFileOutputFormat extends FileOutputFormat<Text, Text> {

        FSDataOutputStream outputStream = null;

        @Override
        public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            final FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            final Path outputPath = new Path(INPUTPATH + File.separator + OUTPUTFILE);
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            outputStream = fileSystem.create(outputPath);

            RecordWriter<Text,Text> recordWriter = new RecordWriter<Text, Text>() {
                @Override
                public void write(Text key, Text value) throws IOException, InterruptedException {
                    outputStream.writeUTF(key.toString());
                }

                @Override
                public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                    outputStream.close();
                }
            };
            return recordWriter;
        }

    }

}
