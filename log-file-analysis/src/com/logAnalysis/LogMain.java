package com.logAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length<2){
			System.out.println("Wrong no of arguments");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Log file analysis");
		
		job.setJarByClass(LogMain.class);
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);
		//job.setNumReduceTasks(0);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
			fs.delete(new Path(args[1]),true);
			//fs.delete(Path(args[1]));
		}
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
;
		
		boolean result = job.waitForCompletion(true);
		if(result)
		{
			Job job1 = Job.getInstance(conf, "no of jobs run on a day");
			
			job1.setJarByClass(LogMain.class);
			job1.setMapperClass(LogMapper1.class);
			job1.setReducerClass(LogReducer1.class);
			//job1.setNumReduceTasks(1);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			
			//FileSystem fs1 = FileSystem.get(conf);
			if(fs.exists(new Path(args[2]))){
				fs.delete(new Path(args[2]),true);
				//fs.delete(Path(args[1]));
			}
			
			FileInputFormat.addInputPath(job1,new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			
			result = job1.waitForCompletion(true); 
			
			if(result)
			{
				Job job2 = Job.getInstance(conf, "elapsed time");
				
				job2.setJarByClass(LogMain.class);
				job2.setMapperClass(LogMapper2.class);
				//job1.setReducerClass(LogReducer1.class);
				//job1.setNumReduceTasks(1);
				
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				
				//FileSystem fs2 = FileSystem.get(conf);
				if(fs.exists(new Path(args[3]))){
					fs.delete(new Path(args[3]),true);
					//fs.delete(Path(args[1]));
				}
				
				FileInputFormat.addInputPath(job2,new Path(args[0]));
				FileOutputFormat.setOutputPath(job2,new Path(args[3]));
				
				result = job2.waitForCompletion(true);
				
				System.exit(result? 0 : 1);
			}
		}
		
	}

}
	