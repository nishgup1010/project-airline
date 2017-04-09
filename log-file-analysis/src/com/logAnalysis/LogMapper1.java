package com.logAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable byteOffset, Text value, Context context)
			throws IOException, InterruptedException {
				
		if (value.toString().contains("starting extract job"))
		{
			String Date[] = value.toString().split(" ");
			String date = Date[4]+" "+Date[5]+" "+Date[6];
			context.write(new Text(date), new IntWritable(1));
		}
		
	}
	

}
