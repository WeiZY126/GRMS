package com.briup.bigdata.project.grms;

import com.briup.bigdata.project.grms.Test8.GrmsBean;
import com.briup.bigdata.project.grms.Test8.SaveRecommendResultToDB;
import com.briup.bigdata.project.grms.step1.UserBuyGoodsList;
import com.briup.bigdata.project.grms.step2.GoodsCooccurrenceList;
import com.briup.bigdata.project.grms.step3.GoodsCooccurrenceMatrix;
import com.briup.bigdata.project.grms.step4.UserBuyGoodsVector;
import com.briup.bigdata.project.grms.step5.MultiplyGoodsMatrixAndUserVector;
import com.briup.bigdata.project.grms.step6.MakeSumForMultiplication;
import com.briup.bigdata.project.grms.step7.DuplicateDataForResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new GoodsRecommendationManagementSystemJobController(), args);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job1 = Job.getInstance(conf, "");
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMap.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1, new Path("/grms/matrix.txt"));
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("/grms/step1/"));

        Job job2 = Job.getInstance(conf, "");
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMap.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2, new Path("/grms/step1/"));
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("/grms/step2/"));

        Job job3 = Job.getInstance(conf, "");
        job3.setJarByClass(this.getClass());
        job3.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMap.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReduce.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3, new Path("/grms/step2/"));
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3, new Path("/grms/step3/"));

        Job job4 = Job.getInstance(conf, "");
        job4.setJarByClass(this.getClass());
        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMap.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReduce.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4, new Path("/grms/matrix.txt"));
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, new Path("/grms/step4/"));

        Job job5 = Job.getInstance(conf, "");
        job5.setJarByClass(this.getClass());
        MultipleInputs.addInputPath(job5, new Path("/grms/step4/"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job5, new Path("/grms/step3/"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        job5.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReduce.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5, new Path("/grms/step5/"));

        Job job6 = Job.getInstance(conf, "");
        job6.setJarByClass(this.getClass());
        job6.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMap.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);
        job6.setCombinerClass(MakeSumForMultiplication.MakeSumForMultiplicationCombiner.class);
        job6.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReduce.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6, new Path("/grms/step5/"));
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6, new Path("/grms/step6/"));

        Job job7 = Job.getInstance(conf, "");
        job7.setJarByClass(this.getClass());
        MultipleInputs.addInputPath(job7, new Path("/grms/matrix.txt"), TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7, new Path("/grms/step6/"), TextInputFormat.class, DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);
        job7.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReduce.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7, new Path("/grms/step7/"));

        Job job8 = Job.getInstance(conf, "cc");
        job8.setJarByClass(this.getClass());
        job8.setMapperClass(SaveRecommendResultToDB.SaveRecommendResultToDBMap.class);
        job8.setMapOutputKeyClass(GrmsBean.class);
        job8.setMapOutputValueClass(IntWritable.class);
        job8.setReducerClass(SaveRecommendResultToDB.SaveRecommendResultToDBReduce.class);
        job8.setOutputKeyClass(GrmsBean.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job8, new Path("/grms/step7/"));
        job8.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job8.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.43.125:3306/grms?serverTimezone=Hongkong", "zs", "123");
        DBOutputFormat.setOutput(job8, "result", "uid","gid","exp");

        ControlledJob cj1 = new ControlledJob(job1.getConfiguration());
        ControlledJob cj2 = new ControlledJob(job2.getConfiguration());
        ControlledJob cj3 = new ControlledJob(job3.getConfiguration());
        ControlledJob cj4 = new ControlledJob(job4.getConfiguration());
        ControlledJob cj5 = new ControlledJob(job5.getConfiguration());
        ControlledJob cj6 = new ControlledJob(job6.getConfiguration());
        ControlledJob cj7 = new ControlledJob(job7.getConfiguration());
        ControlledJob cj8 = new ControlledJob(job8.getConfiguration());
        cj2.addDependingJob(cj1);
        cj3.addDependingJob(cj2);
        cj4.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);
        cj7.addDependingJob(cj6);
        cj8.addDependingJob(cj7);
        JobControl con = new JobControl("");
        con.addJob(cj1);
        con.addJob(cj2);
        con.addJob(cj3);
        con.addJob(cj4);
        con.addJob(cj5);
        con.addJob(cj6);
        con.addJob(cj7);
        con.addJob(cj8);
        Thread t = new Thread(con);
        t.start();
        return 0;
    }
}