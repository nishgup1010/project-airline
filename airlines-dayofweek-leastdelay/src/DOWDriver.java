import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DOWDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Day of week for least delays");
		
		job.setJarByClass(DOWDriver.class);
		job.setMapperClass(DOWMapper.class);
		job.setReducerClass(DOWReducer.class);
		
		job.setCombinerClass(DOWCombiner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);
		
		boolean result = job.waitForCompletion(true);
		
		long endTime = System.currentTimeMillis();
		System.out.println("It took " + (endTime - startTime)/(1000*60) + " minutes");
		System.exit(result?0:1);
	}
}
