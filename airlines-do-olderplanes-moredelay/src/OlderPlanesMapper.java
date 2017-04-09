import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OlderPlanesMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	private static HashMap<String,String> PlaneData = new HashMap<String, String>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		File file = new File("plane-data.csv");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		String lineString="";
		try{
			while((lineString=reader.readLine())!=null){
				String[] flightsDataRow = lineString.split(",");
				if(flightsDataRow.length == 9){
					String tailNumber = flightsDataRow[0].trim().toString();
					String issueDate = flightsDataRow[3].trim().toString();
					PlaneData.put(tailNumber, issueDate);
				}

			}
		} finally{
			if(reader!=null){
				reader.close();
			}
		}	
	}
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String record = value.toString().trim();
		String[] data = record.split(",");
		if (data.length == 29) {
			
			String issueDate="";
			try{
				issueDate = PlaneData.get(data[10]);
			}finally{
				if(issueDate == null){
					issueDate = "NA";
				}
				if(issueDate.equals("")){
					issueDate = "NA";
				}
			}
			
			if(issueDate != "NA"){
				String[] issDate = issueDate.trim().split("/");
				String issueYear="None";
				
				if(issDate.length == 3){
					if(issDate[2]!="" && issDate[2] !=null){
						issueYear = issDate[2];	
					}
				}
				
				long arrivalDelay=(long)0;
				long depDelay=(long)0;
				
				if(data[14].toString() != null || data[15].toString()!=null ||data[14].toString() != "" || data[15].toString()!=""){
					try {
						arrivalDelay = Integer.parseInt(data[14]);
					} catch (NumberFormatException e) {
						return; 
					}
					try {
						depDelay =  Integer.parseInt(data[15]);
					} catch (NumberFormatException e) {
						return;
					}
				}
				if(issueYear != null){
					context.write(new Text(issueYear), new LongWritable(arrivalDelay - depDelay));
				}
			}
		}
		
	}
}
