import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FbShares{
	public static class FbMapper extends MapReduceBase implements 
	Mapper <Object, Text, IntWritable, IntWritable>
	{
	boolean flag = false;
	int i =0;
	public void map(Object Key, Text value, OutputCollector<IntWritable ,IntWritable> output, Reporter reporter) throws IOException
	{
	String line[] = value.toString().split(",",7);
	if(flag){
		String date = line[2];
		//String date1 [] = date.split("/",3);
		int shares = Integer.parseInt(line[5]);
	 	//int d = Integer.parseInt(date1[0]);
		
	if(date.contains("2017"))
		{
			String x[] = date.split("/",3);
			int month =Integer.parseInt(x[0]);
			output.collect(new IntWritable(month), new IntWritable(shares));
		}		
		}
	
	flag=true;
	}
	}


	public static class FbReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>
	{
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable,FloatWritable> output,Reporter reporter) throws IOException
		{
		int add = 0;
		int count = 0;
		while(values.hasNext()){
		
			add = add + values.next().get();
			count++;
			
		}
	
		output.collect(key, new FloatWritable(add/(float)count));
	   }
	  }
	
public static void main(String args[]) throws Exception
{
	JobConf conf = new JobConf(FbShares.class);
	conf.setJobName("Internet Log");
	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(FloatWritable.class);
	conf.setMapOutputKeyClass(IntWritable.class);
	conf.setMapOutputValueClass(IntWritable.class);
	conf.setMapperClass(FbMapper.class);
	conf.setReducerClass(FbReducer.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	JobClient.runJob(conf);
	}
}

	
	
	


