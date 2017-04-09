import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OlderPlanesDriver extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf,"Join example in emp dept table");
		
		job.addCacheFile(URI.create("file:///home/osboxes/sample/flights/plane-data.csv"));
		URI[] cacheFiles=job.getCacheFiles();
		
		if(cacheFiles!=null){
			for(URI cacheFile:cacheFiles){
				System.out.println("Cache Files are : "+ cacheFile);
			}
		}
		System.out.println("After Displaying cache files");
		
		job.setJarByClass(OlderPlanesDriver.class);
		job.setMapperClass(OlderPlanesMapper.class);
		job.setReducerClass(OlderPlanesReducer.class);
		
//		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println("After File Input format");

		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception{
		
		int exitCode = ToolRunner.run(new OlderPlanesDriver(), args);
		System.exit(exitCode);
	}
	
	
}
