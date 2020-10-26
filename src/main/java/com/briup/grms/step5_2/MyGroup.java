package com.briup.bigdata.project.grms.step5_2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    public MyGroup(){
        super(MyKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MyKey m1 = (MyKey)a;
        MyKey m2 = (MyKey)b;
        return m1.getKey().compareTo(m2.getKey());
    }
}
