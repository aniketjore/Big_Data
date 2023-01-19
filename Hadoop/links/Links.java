import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;

public class Links{
	public static class LinkMapper extends Mapper<Object, Text, Text, IntWritable>
	{
	boolean flag = false;

	
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
	String line = value.toString();
	if(flag) {
		StringTokenizer s = new StringTokenizer(line,",");
		String id = s.nextToken();  // id
		String type = s.nextToken();  // type
		
		if(type.equals("link")){
		  String date = s.nextToken();  // retrieved date token
		  int add = 0;
		  
		  while(s.hasMoreTokens()) {
			int num = Integer.parseInt(s.nextToken());
			add += num;
	   }
	   context.write(new Text(date), new IntWritable(add));
         }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
	}
	flag = true;
     }
}
	/*public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
		IntWritable addition = new IntWritable();
		int sum = 0;
		for (IntWritable num : values)
		{
		sum = sum + num.get();
		}
		addition.set(sum);
		context.write(key, addition);
		}
	   } */
	
public static void main(String args[]) throws Exception
{
	//create the object of Configyration class
	Configuration conf = new Configuration();
	
	// create the object of job class
	Job job = new Job(conf, "LinkCount");
	
	// Set the data type of output key
	job.setOutputKeyClass(Text.class);
	
	// Set the data type of otuput value
	job.setOutputValueClass(IntWritable.class);
	
	// Set the data format of output
	job.setOutputFormatClass(TextOutputFormat.class);
	
	// Set the data format of input
	job.setInputFormatClass(TextInputFormat.class);
	
	// Set the name of Mappper class
	job.setMapperClass(LinkMapper.class);
	job.setNumReduceTasks(0); // Reducers = 0;
	// Set the name of Reducer class
	//job.setReducerClass(WordReducer.class);
	// Set the input files path from 0th argument
	FileInputFormat.addInputPath(job, new Path(args[0]));
	
	// Set the output files path from 1st argument
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	// Execute the job and wait for completion
	job.waitForCompletion(true);
	}
}

	
	
	


