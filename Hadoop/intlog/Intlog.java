import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
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

public class Intlog{
	public static class LogMapper extends Mapper<Object, Text, Text, FloatWritable>
	{
	
	
	
	public void map(Object Key, Text value, Context context) throws IOException,InterruptedException
	{
	String line = value.toString();
	StringTokenizer st = new StringTokenizer(line,"	");
	String name = st.nextToken();
	int sum = 0;
	while(st.hasMoreTokens())
	{
		int m = Integer.parseInt(st.nextToken());
		sum = sum + m;
		//context.write(new Text(name), new FloatWritable(sum/7));
	}
	float avgtime = sum/7.0f;
	context.write(new Text(name), new FloatWritable(avgtime));
	
	}
	}


	public static class LogReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
		{
		FloatWritable hrs = new FloatWritable();
		for(FloatWritable x : values)
		{
		if (x.get() > 5)
		{
			hrs.set(x.get());
		}

		}
		context.write(key,hrs);
	   }
	  }
	
public static void main(String args[]) throws Exception
{
	//create the object of Configyration class
	Configuration conf = new Configuration();
	
	
	// create the object of job class
	Job job = new Job(conf, "Ages");
	
	//Set the data type of output key
	job.setMapOutputValueClass(Text.class);
	
	// Set the data type of otuput value
	job.setMapOutputValueClass(IntWritable.class);
	
	// Set the data type of output key
	job.setOutputKeyClass(Text.class);
	
	// Set the data type of otuput value
	job.setOutputValueClass(FloatWritable.class);
	
	// Set the data format of output
	job.setOutputFormatClass(TextOutputFormat.class);
	
	// Set the data format of input
	job.setInputFormatClass(TextInputFormat.class);
	
	// Set the name of Mappper class
	job.setMapperClass(LogMapper.class);
	
	// Set the name of Reducer class
	job.setReducerClass(LogReducer.class);
	// Set the input files path from 0th argument
	FileInputFormat.addInputPath(job, new Path(args[0]));
	
	// Set the output files path from 1st argument
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	// Execute the job and wait for completion
	job.waitForCompletion(true);
	}
}

	
	
	


