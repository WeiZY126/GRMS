package com.briup.bigdata.project.grms.step5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiplyGoodsMatrixAndUserVector {

    public static class MultiplyGoodsMatrixAndUserVectorFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]), new Text("a" + split[1]));
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]), new Text("b" + split[1]));
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String start = null;
            String end = null;
            for (Text value : values) {
                if (value.toString().startsWith("a"))
                    start = value.toString().substring(1, value.toString().length());
                else if (value.toString().startsWith("b"))
                    end = value.toString().substring(1, value.toString().length());
            }
            String[] userAndNums = start.split("[,]");
            String[] goodAndGoods = end.split("[,]");
            for (String userAndNum : userAndNums) {
                String[] uAN = userAndNum.split("[:]");
                String user = uAN[0];
                int num = Integer.parseInt(uAN[1]);
                for (String goodAndGood : goodAndGoods) {
                    String[] gAG = goodAndGood.split("[:]");
                    String good = gAG[0];
                    int likeNum = Integer.parseInt(gAG[1]);
                    context.write(new Text(user + "," + good), new Text(String.valueOf(num * likeNum)));
                }
            }
        }
    }
}