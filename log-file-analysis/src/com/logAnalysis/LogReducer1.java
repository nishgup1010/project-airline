package com.logAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text date, Iterable<IntWritable> count,Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable number: count)
		{
			sum = sum + number.get();
		}
		context.write(new Text("no. of times job run on "+date+" is: "), new IntWritable(sum));
	}
}
