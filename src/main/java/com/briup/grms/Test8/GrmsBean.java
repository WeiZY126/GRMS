package com.briup.bigdata.project.grms.Test8;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GrmsBean implements WritableComparable, DBWritable {
    private String uid;
    private String gid;
    private int exp;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }

    public GrmsBean() {
    }

    public GrmsBean(String uid, String gid, int exp) {
        this.uid = uid;
        this.gid = gid;
        this.exp = exp;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(gid);
        out.writeInt(exp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        uid=in.readUTF();
        gid=in.readUTF();
        exp=in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1,uid);
        statement.setString(2,gid);
        statement.setInt(3,exp);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        uid=resultSet.getString(1);
        gid=resultSet.getString(2);
        exp=resultSet.getInt(3);
    }

    @Override
    public String toString() {
        return "GrmsBean{" +
                "uid='" + uid + '\'' +
                ", gid='" + gid + '\'' +
                ", exp=" + exp +
                '}';
    }
}
