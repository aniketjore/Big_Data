import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FbLive{
	public static class FbMapper extends MapReduceBase implements 
	Mapper <Object, Text, Text, IntWritable>
	{
	boolean flag = false;
	public void map(Object Key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException
	{
	
	
	if(flag){
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line,",");
		String id = st.nextToken();
		String type  = st.nextToken();
		String date = st.nextToken();
		int count = 0 , like=0;
		while(count < 4)
		{
			like = Integer.parseInt(st.nextToken());
			count++;
		}
	if(date.startsWith("2") && date.contains("2018") && type.equals("video"))
		{
		output.collect(new Text("Likes"), new IntWritable(like));
		}
	
	}
	flag=true;
	}
	}


	public static class FbReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException
		{
		int add = 0;
		while(values.hasNext())
			add = add + values.next().get();
		output.collect(key, new IntWritable(add));
		
	   }
	  }
	
public static void main(String args[]) throws Exception
{
	JobConf conf = new JobConf(FbLive.class);
	conf.setJobName("Internet Log");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(FbMapper.class);
	conf.setReducerClass(FbReducer.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	JobClient.runJob(conf);
	}
}

	
	
	


