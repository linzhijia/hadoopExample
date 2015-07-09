package com.test1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class test1 extends Configured implements Tool {

	enum Counter
	{
		LINESKIP,//�������
	}
	
	
	public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString(); //��ȡԴ����
			try{
				//���ݴ���
				String[] lineSplit = line.split(" ");
				String month = lineSplit[0];
				String time = lineSplit[1];
				String mac = lineSplit[6];
				Text out = new Text(month+" "+time+" "+mac);
				context.write(NullWritable.get(), out);//���  key \t value
			}catch(java.lang.ArrayIndexOutOfBoundsException e){
				context.getCounter(Counter.LINESKIP).increment(1);//����������� +1 
				return;
			}
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,"test 1");//������ 
		job.setJarByClass(test1.class);//ָ��Class
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));//����·��
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));//���·��
		
		job.setMapperClass(Map.class);//���������Map����ΪMap�������
		job.setOutputFormatClass(TextOutputFormat.class); //org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
		job.setOutputKeyClass(NullWritable.class); //���key��ʽ
		job.setOutputValueClass(Text.class);//���value�ĸ�ʽ
		
		job.waitForCompletion(true);
		return job.isSuccessful()?0:1;
	}

	public static void main(String[] args) throws Exception{
		//��������
		int res = ToolRunner.run(new Configuration(), new test1(),args);
		System.exit(res);
	}
}
