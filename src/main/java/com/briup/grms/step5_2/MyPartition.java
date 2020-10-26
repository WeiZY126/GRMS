package com.briup.bigdata.project.grms.step5_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<MyKey, Text> {
    /*
    要求：
    必须让20001,0和20001,1进入同一分区
     */
    @Override
    public int getPartition(MyKey myKey, Text text, int numPartitions) {
        int i = Integer.parseInt(myKey.getKey());
        return i%numPartitions;
    }
}
