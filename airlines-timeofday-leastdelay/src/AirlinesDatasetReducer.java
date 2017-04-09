import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AirlinesDatasetReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count = 0;
		int avg = 0;
		int sum = 0;
		
		for(IntWritable val:values)
		{
			sum = sum + val.get();
			count ++;
		}
			avg = sum/count;
		context.write(key, new IntWritable(avg));
	}
}
