package com.logAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text,DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text timeStamp, Iterable<DoubleWritable> elapsedTime, Context context)
			throws IOException, InterruptedException {
		Double avgTime = 0.0;
		int count=0;
		DecimalFormat df = new DecimalFormat("##.##");
		
		for(DoubleWritable time: elapsedTime)
		{
			if(Double.parseDouble(time.toString())!=0.0)
			{
			count++;
			avgTime+= Double.parseDouble(time.toString());
			}
		}
		context.write(new Text("Avg. time taken on "+timeStamp+" is"), new DoubleWritable(Double.parseDouble(df.format((avgTime/Double.parseDouble(count+".0"))))));
	}
	
	
	

}