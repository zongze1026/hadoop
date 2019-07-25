package com.zongze.serialize;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.*;

/**
 * Create By xzz on 2019/7/24
 * hadoop串行化
 */
public class HadoopSerialize {

    public static void main(String[] args) throws IOException {
//        intSerialize();
//        textSerialize();
//        arraySerialize();
        objectSerialize();



    }

    //自定义串行化
    private static void objectSerialize() throws IOException {
        Person person = new Person("tom", 15, false);
        PersonWritable writable = new PersonWritable();
        writable.setPerson(person);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writable.write(new DataOutputStream(baos));

        writable = new PersonWritable();
        writable.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        Person per = writable.getPerson();
        System.out.println(per);


    }

    private static void arraySerialize() throws IOException {
        ArrayWritable writable = new ArrayWritable(Text.class);
        Text[] arrays = {new Text("hellow"),new Text("中国")};
        writable.set(arrays);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writable.write(new DataOutputStream(out));
        //unSerialize
        writable = new ArrayWritable(Text.class);
        writable.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
        Writable[] writables = writable.get();
        for (Writable text:writables){
            System.out.println(text);
        }

    }

    private static void textSerialize() {
        Text text = new Text("中国");
        //返回字节数组的长度
        int len1 = text.getLength();
        System.out.println(len1);
        //返回capacity容量
        int len2 = text.getBytes().length;
        System.out.println(len2);



    }

    private static void intSerialize() throws IOException {
        //串行过程
        IntWritable writable = new IntWritable(100);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        writable.write(out);
        out.close();
        //反串行过程
        byte[] bytes = bos.toByteArray();
        writable = new IntWritable();
        writable.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
        int i = writable.get();
        System.out.println(i);
    }


}
