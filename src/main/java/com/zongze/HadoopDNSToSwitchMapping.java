package com.zongze;

import org.apache.hadoop.net.DNSToSwitchMapping;
import java.util.ArrayList;
import java.util.List;

/**
 * Create By xzz on 2019/7/22
 * 自定义实现hadoop机架感知
 * 机架分组
 */
public class HadoopDNSToSwitchMapping implements DNSToSwitchMapping {
    @Override
    public List<String> resolve(List<String> names) {
        List<String> racks = new ArrayList<>();
        if (null != names && names.size() > 0) {
            Integer ip;
            for (String name : names) {
                if (name.startsWith("s")) {  //判断参数是host名称还是ip
                    ip = Integer.valueOf(name.substring(1));
                } else {
                    ip = Integer.valueOf(name.substring(name.lastIndexOf(".") + 1));
                }
                //获取到主机;进行机架分组
                if (ip <= 203) {
                    racks.add("/rack1/s" + ip);
                } else {
                    racks.add("/rack2/s" + ip);
                }
            }

        }
        return racks;
    }

    @Override
    public void reloadCachedMappings() {

    }

    @Override
    public void reloadCachedMappings(List<String> names) {

    }
}
