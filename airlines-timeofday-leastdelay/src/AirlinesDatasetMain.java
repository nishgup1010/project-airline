import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirlinesDatasetMain{

	public static void main(String[] args) throws  IOException, ClassNotFoundException, InterruptedException 
	{
		if(args.length<2){
			System.out.println("Wrong no of arguments");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Sample MR program");
		
		job.setJarByClass(AirlinesDatasetMain.class);
		job.setMapperClass(AirlinesDatasetMapper.class);
		job.setReducerClass(AirlinesDatasetReducer.class);
		
		job.setOutputValueClass(IntWritable.class); 
		job.setOutputKeyClass(Text.class);
		
		Date now = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("hh-mm-ss");
		String time = dateFormat.format(now);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+ "_" + time));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
		
	}
}