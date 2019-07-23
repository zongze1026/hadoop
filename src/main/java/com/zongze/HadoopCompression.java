package com.zongze;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.*;

/**
 * Create By xzz on 2019/7/23
 * <p>
 * hadoop压缩相关
 */
public class HadoopCompression {


    public static void main(String[] args) throws IOException {

        Class[] classes = {DefaultCodec.class, GzipCodec.class, BZip2Codec.class, SnappyCodec.class,Lz4Codec.class};
        //文件从外部传入
        String sourcePath = "C:\\Users\\Administrator\\Desktop\\soft-gz\\aa.doc";
        Configuration configuration = new Configuration();
        for (Class clazz : classes) {
            compress(clazz, configuration, sourcePath);
            unCompress(clazz,configuration,sourcePath);
        }
    }

    private static void unCompress(Class clazz, Configuration configuration, String sourcePath) {
        try {
            long star = System.currentTimeMillis();
            //获取到压缩api实例
            CompressionCodec compress = (CompressionCodec) ReflectionUtils.newInstance(clazz, configuration);
            //获取加压器
            Decompressor decompressor = compress.createDecompressor();
            //获取压缩后缀名称
            String extName = compress.getDefaultExtension();
            CompressionInputStream in = compress.createInputStream(new FileInputStream(sourcePath + extName), decompressor);
            IOUtils.copyBytes(in,new FileOutputStream(sourcePath+"un"+extName),1024);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void compress(Class clazz, Configuration configuration, String sourcePath) {
        try {
            long star = System.currentTimeMillis();
            //获取到压缩api实例
            CompressionCodec compress = (CompressionCodec) ReflectionUtils.newInstance(clazz, configuration);
            //获取压缩后缀名称
            String extName = compress.getDefaultExtension();
            //压缩文件名称加上扩展名放在当前目录下面
            File file = new File(sourcePath + extName);
            CompressionOutputStream out = compress.createOutputStream(new FileOutputStream(file));
            IOUtils.copyBytes(new FileInputStream(sourcePath), out, 1024);
            System.out.println(extName + "=" + (System.currentTimeMillis() - star));
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
