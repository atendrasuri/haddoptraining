package region_earthquak;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
public class RegionEarthQuak extends Configured implements Tool {
 public static void main(String[] args) throws Exception {
        if (args.length != 2) {
          System.out.println("usage: [input] [output]");
          System.exit(-1);
        }
   
        Configuration conf = new Configuration();
        conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.GZipCodec");
   
        Job job = Job.getInstance(conf);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
 
        job.setMapperClass(RegionMapper.class); 
        job.setCombinerClass(RegionReducer.class);
        job.setReducerClass(RegionReducer.class); 
       
       
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(2);
        //job.setPartitionerClass(CustomPartition.class);
 
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.setJarByClass(RegionEarthQuak.class);
 
        job.submit();
         
         
         
   
 
   
 }

@Override
public int run(String[] arg0) throws Exception {
	// TODO Auto-generated method stub
	return 0;
}
}