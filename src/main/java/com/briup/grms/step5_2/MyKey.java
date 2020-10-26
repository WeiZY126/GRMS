package com.briup.bigdata.project.grms.step5_2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {
    private String key; //商品id
    /**
     * 用来给不同的value定义先后顺序
     * 20001 10004:1,10001:1，10005:1
     * 20001 20005:2,20002:2，20001:3,20007:1,20006,2
     *
     * 20001,0 10004:1,10001:1，10005:1
     * 20001,1 20005:2,20002:2，20001:3,20007:1,20006,2
     *
     * 问题：分区 flag不同会进入不同分区
     *      分组 flag不同会进入不同分区
     */
    private int flag;

    public MyKey(String key, int flag) {
        this.key = key;
        this.flag = flag;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public MyKey() {
    }

    @Override
    public String toString() {
        return "MyKey{" +
                "key='" + key + '\'' +
                ", flag=" + flag +
                '}';
    }

    @Override
    public int compareTo(MyKey o) {
        int k = key.compareTo(o.key);
        if (k==0){
            return flag-o.flag;
        }else {
            return k;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.key=in.readUTF();
        this.flag=in.readInt();
    }
}
