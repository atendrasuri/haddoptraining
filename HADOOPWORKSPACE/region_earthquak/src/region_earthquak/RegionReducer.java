package region_earthquak;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class RegionReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable maxmagnitude = new DoubleWritable();

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<DoubleWritable> it=values.iterator();
		Double max= 0d;
		while (it.hasNext()) {
			double m=it.next().get();
			if(m>max){
				max=m;
			}
		}
		maxmagnitude.set(max);

		context.write(key, maxmagnitude);
	}
}
