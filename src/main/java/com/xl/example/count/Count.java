package com.xl.example.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xl.Sort;

public class Count {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: Word count <in> <out>");
			System.exit(2);
		}
		System.out.println(args[0]);
		System.out.println(args[1]);

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Count.class);

		// 设置Map和Reduce处理类
		job.setMapperClass(CountMap.class);
		job.setReducerClass(CountReduce.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
