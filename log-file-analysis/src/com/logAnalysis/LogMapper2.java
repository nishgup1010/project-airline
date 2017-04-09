package com.logAnalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper2 extends Mapper<LongWritable, Text, Text, Text>{
		int count = 0;
		int SuccessfulJob = 0;
		StringBuffer singleLog	 = new StringBuffer();

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			context.write(new Text("JOB RUN ON"+"\t"+"\t"+"\t"+"\t"), new Text("TIME TAKEN"+"\r\n"));
		}

		@Override
		protected void map(LongWritable byteOffset, Text value, Context context)
				throws IOException, InterruptedException {
			String elapsedTime="0";	
			if (!value.toString().contains("==========================================="))
			{
				singleLog.append(value+" ");
			}
			else
			{
				count ++;
				String[] data = singleLog.toString().split(" ");
				String timeStamp = data[4]+" "+data[5]+" "+data[6]+" "+data[7]+" "+data[8]+" "+data[9];
				for(int i=0;i<data.length; i++)
				{
					if(data[i].contains("Elapsed:"))
					{
						elapsedTime = data[i+1];
						SuccessfulJob++;
					}
				}
				singleLog.delete(0, singleLog.length());
				context.write(new Text("Job run on "+timeStamp+"\t"), new Text(elapsedTime));
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			context.write(new Text("\n"+"no.of times job started is: "), new Text(count+"\r\n"+"no.of successful job is: "+SuccessfulJob+"\r\n"+"no.of UnSuccessful job is: "+(count-SuccessfulJob)+"\r\n"));
		}
		
		
}

