/*******************************************************************************
 * @(#)MapReduceTest.java 2015年10月30日
 *
 * Copyright 2015 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.example.hadoop.mapreduce.test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.example.hadoop.hdfs.test.HdfsClient;

/**
 * @author <a href="mailto:pud@emrubik.com">pu dong</a>
 * @version $Revision 1.0 $ 2015年10月30日 下午3:34:59
 */
public class MapReduceTest {

    private static final String HDFS_PATH = "hdfs://192.168.1.200:9000";

    static {
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    public static class MapperTest extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        private static final String THE = "the";

        private IntWritable one = new IntWritable(1);

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
                Reporter reporter) throws IOException {
            System.out.println("key:" + key);
            String str = value.toString();
            if (str.contains(THE)) {
                word.set(THE);
                output.collect(word, one);
            }
        }
    }

    public static class ReducerTest extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += ((IntWritable) values.next()).get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws IOException {
        String input = HDFS_PATH + "/input/README.txt";
        String input2 = HDFS_PATH + "/input/README2.txt";
        String output = HDFS_PATH + "/test/output";

        // 如果输入文件夹已经存在，则执行mapreduce任务时会提示文件夹已存在的错误，所以如果已经存在，则删除掉
        if (HdfsClient.exists(output)) {
            HdfsClient.rm(output);
        }

        JobConf conf = new JobConf(MapReduceTest.class);
        conf.setJobName("MapReduceTest");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        // 设置mapper的输出类型
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        // 设置reducer的输出类型
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // 设置mapper
        conf.setMapperClass(MapperTest.class);
        // 设置combiner，为了提高效率，先在每台服务器的本地对mapper的执行结果做一次合并，通常与reducer的逻辑相同
        conf.setCombinerClass(ReducerTest.class);
        // 设置reducer
        conf.setReducerClass(ReducerTest.class);

        // 设置MapReduce输入文件的解析类
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // 设置MapReduce的输入源
        FileInputFormat.setInputPaths(conf, new Path[] { new Path(input), new Path(input2) });
        // 设置MapReduce的输出源
        FileOutputFormat.setOutputPath(conf, new Path(output));

        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
