package com.jxlg.hadoop.project.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.step1
 * @filename: UserBuyGoodsList.java
 * @create: 2019/10/12 11:13
 * @author: 29314
 * @description: .用户购买商品的列表
 **/

public class UserBuyGoodsList extends Configured implements Tool {
    public static class UserBuyGoodsListMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text k2 = new Text();
        private Text v2 = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[\t]");
            if(Integer.parseInt(strs[2])>0){
                this.k2.set(strs[0]);
                this.v2.set(strs[1]);
                context.write(this.k2,this.v2);
            }
        }
    }

    public static class UserBuyGoodsListReducer extends Reducer<Text, Text, Text, Text> {
        private Text k3 = new Text();
        private Text v3 = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            this.k3.set(key.toString());
            StringBuilder sb = new StringBuilder();
            for(Text goods:values){
                sb.append(goods.toString()).append(",");
            }
            this.v3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //数据来源于原始数据
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "计算用户购买商品的列表");
        job.setJarByClass(this.getClass());

        job.setMapperClass(UserBuyGoodsListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(UserBuyGoodsListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsList(),args));
    }
}
