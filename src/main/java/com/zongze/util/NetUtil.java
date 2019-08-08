package com.zongze.util;

import org.apache.commons.lang.StringUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Create By xzz on 2019/8/5
 */
public class NetUtil {


    public static void main(String[] args) {
        System.out.println(getHostInfo(NetUtil.class, "工具测试"));
        System.out.println(getHostInfo(NetUtil.class, null));
    }


    public static String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getPID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String name = runtimeMXBean.getName();
        return name.substring(0, name.indexOf("@"));
    }

    public static String getThreadName() {
        return Thread.currentThread().getName();
    }

    public static String getHostInfo(Class clazz, String message) {
        String className = clazz.getSimpleName();
        StringBuffer buffer = new StringBuffer("[");
        buffer.append(className).append(":").append(getHost()).append(":").append(getPID()).append(":").append(getThreadName());
        if (StringUtils.isNotBlank(message)) {
            buffer.append(":").append(message);
        }
        buffer.append("]");
        return buffer.toString();
    }


}
