import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlinesDatasetMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Text firstLine = null;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
			if(firstLine!=null)
			{
				String[] line = value.toString().split(",");
				
				if(line.length==29 && !(line[15].equalsIgnoreCase("NA")) && !(line[5].equalsIgnoreCase("NA")))
				{
					String minutues = "00";
					String hours = "00";
					String deptTime = line[5];
					if(deptTime.length()>2)
					{
						minutues = line[5].substring(line[5].length()-2, line[5].length());
						hours = line[5].substring(0,line[5].length()-2);
					}
					else if(deptTime.length()<=2)
					{
						minutues = deptTime;
						hours = "00";
					}
					context.write(new Text(hours),new IntWritable(Integer.parseInt(line[15])));
				}
			}
			if(firstLine==null)
			{
				firstLine = value;
			}
	}
}
