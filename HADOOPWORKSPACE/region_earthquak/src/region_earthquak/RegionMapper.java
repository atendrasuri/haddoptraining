
package region_earthquak;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RegionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
 private Text region = new Text();
 
 private final static DoubleWritable magnitude = new DoubleWritable(1);
 
  
 @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
	  // Break line into words for processing
		 //if(key.get()!=0){
			if(!value.toString().startsWith("Src")) {
	      region.set(value.toString().split(",")[11]);
	      magnitude.set(Double.parseDouble(value.toString().split(",")[8]));
	     
	         context.write(region, magnitude);
		 }
	  
	 }
}
