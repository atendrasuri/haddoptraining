package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartition extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int nooreducer) {
		
		
		char firstletter= key.toString().trim().charAt(0);
		System.out.println(nooreducer+" "+firstletter);
		
		if(firstletter-'A'>=0  && firstletter-'A'<=13){
			
			return 0;
		}
		else
		{
			return 1;
		}
		
		// TODO Auto-generated method stub
		//return 0;
	}

}
