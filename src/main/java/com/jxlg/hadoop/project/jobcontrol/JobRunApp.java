package com.jxlg.hadoop.project.jobcontrol;

import com.jxlg.hadoop.project.step1.UserBuyGoodsList;
import com.jxlg.hadoop.project.step2and3.GoodsCooccurrenceMatrix;
import com.jxlg.hadoop.project.step4.UserBuyGoodsVector;
import com.jxlg.hadoop.project.step5.KeyValueData;
import com.jxlg.hadoop.project.step5.MultiplyGoodsMatrixAndUserVector;
import com.jxlg.hadoop.project.step7.DuplicateDataForResult;
import com.jxlg.hadoop.project.step7.UserGoodsData;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.jobcontrol
 * @filename: JobControl.java
 * @create: 2019/10/13 11:31
 * @author: 29314
 * @description: .所有作业配置
 **/

public class JobRunApp extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //原始数据
        Path in = new Path(conf.get("in"));//步骤1，4，7的输入
        Path out1 = new Path(conf.get("out1"));//步骤1的输出     步骤2和3的输入
        Path out2 = new Path(conf.get("out2"));//步骤2和3的输出   步骤5的输入
        Path out3 = new Path(conf.get("out3"));//步骤4的输出     步骤5的输入
        Path out4 = new Path(conf.get("out4"));//步骤5的输出     步骤6的输入
        Path out5 = new Path(conf.get("out5"));//步骤6的输出     步骤7的输入

        //作业配置
        System.out.println("作业1配置开始——————————————————");
        Job job1 = Job.getInstance(conf, "计算用户购买商品的列表");
        job1.setJarByClass(this.getClass());

        job1.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,in);

        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job1,out1);
        System.out.println("作业1配置结束——————————————————");


        System.out.println("作业2配置开始——————————————————");
        Job job2 = Job.getInstance(conf, "计算商品的共现关系与共现次数(共现矩阵)");
        job2.setJarByClass(this.getClass());

        job2.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job2,out1);

        job2.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job2,out2);
        System.out.println("作业2配置结束——————————————————");

        System.out.println("作业4配置开始——————————————————");

        Job job3 = Job.getInstance(conf, "计算用户的购买向量");
        job3.setJarByClass(this.getClass());

        job3.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,in);

        job3.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job3,out3);
        System.out.println("作业4配置结束——————————————————");

        System.out.println("作业5配置开始——————————————————");

        Job job4 = Job.getInstance(conf, "商品共现矩阵乘以用户购买向量");
        job4.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(
                job4,out3, SequenceFileInputFormat.class, MultiplyGoodsMatrixAndUserVector.FirstMapper.class);
        MultipleInputs.addInputPath(
                job4,out2,SequenceFileInputFormat.class, MultiplyGoodsMatrixAndUserVector.SecondMapper.class);
        //设置mapper的输出key，value的数据类型
        job4.setMapOutputKeyClass(KeyValueData.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setGroupingComparatorClass(MultiplyGoodsMatrixAndUserVector.MyGroupingComparator.class);
        job4.setSortComparatorClass(MultiplyGoodsMatrixAndUserVector.MySortComparator.class);

        job4.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job4,out4);
        System.out.println("作业5配置结束——————————————————");

        System.out.println("作业6配置开始——————————————————");
        Job job5 = Job.getInstance(conf, "计算的推荐的零散结果进行求和");
        job5.setJarByClass(this.getClass());

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(IntWritable.class);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job5,out4);

        job5.setReducerClass(IntSumReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job5,out5);
        System.out.println("作业6配置结束——————————————————");

        System.out.println("作业7配置开始——————————————————");
        Job job6 = Job.getInstance(conf, "在推荐结果中去掉用户已购买的商品信息,并入库");
        job6.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(
                job6,in, TextInputFormat.class, DuplicateDataForResult.FirstMapper.class);
        MultipleInputs.addInputPath(
                job6,out5,SequenceFileInputFormat.class, DuplicateDataForResult.SecondMapper.class);
        //设置mapper的输出key，value的数据类型
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(IntWritable.class);

        job6.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job6.setOutputKeyClass(UserGoodsData.class);
        job6.setOutputValueClass(NullWritable.class);
        job6.setOutputFormatClass(DBOutputFormat.class);
        //配置数据库连接信息
        DBConfiguration.configureDB(
                job6.getConfiguration(),
                "com.mysql.cj.jdbc.Driver","jdbc:mysql://192.168.112.121:8017/db_test",
                "root","root"
        );
        //配置数据输出信息
        DBOutputFormat.setOutput(job6,"tbl_ugr","userid","goodsid","level");
        System.out.println("作业7配置结束——————————————————");

        //将不可控制的job变为可控制
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);
        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);
        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);
        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);
        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);
        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);

        /*Path in = new Path(conf.get("in"));//步骤1，4，7的输入
        Path out1 = new Path(conf.get("out1"));//步骤1的输出     步骤2和3的输入
        Path out2 = new Path(conf.get("out2"));//步骤2和3的输出   步骤5的输入
        Path out3 = new Path(conf.get("out3"));//步骤4的输出     步骤5的输入
        Path out4 = new Path(conf.get("out4"));//步骤5的输出     步骤6的输入
        Path out5 = new Path(conf.get("out5"));//步骤6的输出     步骤7的输入*/
        //添加依赖关系
        cj2.addDependingJob(cj1);
        cj4.addDependingJob(cj2);
        cj4.addDependingJob(cj3);
        cj5.addDependingJob(cj4);
        cj6.addDependingJob(cj5);

        JobControl jc = new JobControl("作业控制");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        //提交作业
        Thread t = new Thread(jc);
        t.start();
        System.out.println("所有作业配置完成");
        do{
            for(ControlledJob j: jc.getRunningJobList()){
                j.getJob().monitorAndPrintJob();
            }
        }while(!jc.allFinished());

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new JobRunApp(),args));
    }
}
