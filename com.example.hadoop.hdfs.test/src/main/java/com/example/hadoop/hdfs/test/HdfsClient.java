/*******************************************************************************
 * @(#)HdfsClient.java 2015年11月2日
 *
 * Copyright 2015 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.example.hadoop.hdfs.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author <a href="mailto:pud@emrubik.com">pu dong</a>
 * @version $Revision 1.0 $ 2015年11月2日 上午11:48:24
 */
public class HdfsClient {

    // HDFS访问地址
    private static final String HDFS_URL = "hdfs://192.168.1.200:9000/";

    // private static final HdfsClient CLIENT = new HdfsClient();

    // public static HdfsClient getInstance() {
    // return CLIENT;
    // }

    private static Configuration conf;

    private static FileSystem fs;

    private HdfsClient() {
    }

    static {
        System.setProperty("HADOOP_USER_NAME", "root");
        conf = config();
        try {
            fs = FileSystem.get(URI.create(HDFS_URL), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JobConf config() {
        JobConf conf = new JobConf(HdfsClient.class);
        conf.setJobName("HdfsClient");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    public static void ls(String folder) throws FileNotFoundException, IOException {
        Path path = new Path(folder);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(),
                    f.getLen());
        }
        System.out.println("==========================================================");
    }

    public static boolean exists(String folder) throws IOException {
        Path path = new Path(folder);
        return fs.exists(path);
    }

    public static boolean mkdirs(String folder) throws IOException {
        boolean result = false;
        Path path = new Path(folder);
        if (!fs.exists(path)) {
            result = fs.mkdirs(path);
            System.out.println("Create: " + folder + " successfully.");
        } else {
            System.out.println("folder:" + folder + " is already exist.");
        }
        return result;
    }

    public static boolean rm(String file) throws IOException {
        boolean result = false;
        Path path = new Path(file);
        // 如果传入的参数file是一个目录，则自动删除该目录下的所有文件
        result = fs.delete(path, true);
        System.out.println("remove:" + file + " successfully.");
        return result;
    }

    public static void main(String[] args) {
        try {
            HdfsClient.rm("/test2");
            HdfsClient.rm("/tmp");
            HdfsClient.rm("/user");
            // HdfsClient.rm("/test");
            // HdfsClient.ls("/test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
