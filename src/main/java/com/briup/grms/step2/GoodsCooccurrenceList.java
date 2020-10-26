package com.briup.bigdata.project.grms.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GoodsCooccurrenceList {

    public static class GoodsCooccurrenceListMap extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]")[1].split("[,]");
            for (int i = 0; i < split.length; i++) {
                for (int j = 0; j < split.length; j++) {
                    context.write(new Text(split[i]),new Text(split[j]));
                }
            }
        }
    }

}