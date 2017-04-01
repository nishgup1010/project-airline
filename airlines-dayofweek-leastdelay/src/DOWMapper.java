import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DOWMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String data = value.toString();
		String[] values = data.split(",");
		if (values.length == 29) {
			String dayOfWeek = values[3];
			Double delayInDep = 0.0;
			
			try {
				delayInDep = Double.parseDouble(values[15]);
			} catch (NumberFormatException e) {
				return; 
			}
			
			context.write(new Text(dayOfWeek), new DoubleWritable(delayInDep));
			
		}	
	}
}
