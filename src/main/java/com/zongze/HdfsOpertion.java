package com.zongze;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Create By xzz on 2019/7/18
 */
public class HdfsOpertion {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        fs.setVerifyChecksum(false);   //是否启用校验和

//        putFile(fs);

//        setReplication(fs);

        getFile(fs);

//        deleteFile(fs);

//        listFileSystem(fs, new Path("/"));


    }

    private static void setReplication(FileSystem fs) throws IOException {
        Path path = new Path("/usr/root/hadoop/bb.txt");
        fs.setReplication(path, (short) 3);
    }

    private static void listFileSystem(FileSystem fs, Path path) throws IOException {
        System.out.println(path.toUri().toString());
        if (fs.isDirectory(path)) {
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (null != fileStatuses && fileStatuses.length > 0) {
                for (FileStatus fileStatus : fileStatuses) {
                    Path path1 = fileStatus.getPath();
                    listFileSystem(fs,path1);
                }
            }
        }


    }

    private static void deleteFile(FileSystem fs) throws IOException {
        Path path = new Path("/usr/root/hadoop/aa.tar.gz");
        fs.delete(path, true);
    }

    private static void getFile(FileSystem fs) throws IOException {
        Path path = new Path("/usr/root/hadoop/dd.txt");
        FSDataInputStream inputStream = fs.open(path);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copyBytes(inputStream, outputStream, 1024);
        System.out.println(new String(outputStream.toByteArray(), "utf-8"));
    }

    private static void putFile(FileSystem fs) throws IOException {
        FSDataOutputStream fos = fs.create(new Path("/usr/root/hadoop/u.ini"), true, 1024, (short) 3, 1048576);
        IOUtils.copyBytes(new FileInputStream("C:\\Users\\Administrator\\Desktop\\config\\u.ini"), fos, 1024);
    }


}
