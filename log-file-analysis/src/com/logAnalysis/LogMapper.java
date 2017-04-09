package com.logAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	StringBuffer singleLog	 = new StringBuffer();
	
	@Override
	protected void map(LongWritable byteOffset, Text value, Context context)
			throws IOException, InterruptedException {
		Double elapsedTime = 0.0;	
		if (!value.toString().contains("==========================================="))
		{
			singleLog.append(value+" ");
		}
		else
		{
			String[] data = singleLog.toString().split(" ");
			String timeStamp = data[5]+" "+data[6];
			for(int i=0;i<data.length; i++)
			{
				if(data[i].contains("Elapsed:"))
				{
					elapsedTime = Double.parseDouble(data[i+1]);
				}
			}
			singleLog.delete(0, singleLog.length());
			context.write(new Text(timeStamp), new DoubleWritable(elapsedTime));
		}
	}

}