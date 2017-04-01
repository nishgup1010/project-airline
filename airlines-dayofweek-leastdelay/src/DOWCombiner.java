import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DOWCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		long totalDelay = 0;
		for (DoubleWritable value : values) {
			totalDelay = totalDelay + (long) (value.get()); 																			
		}
		context.write(key,new DoubleWritable(totalDelay));	
	}	
}
