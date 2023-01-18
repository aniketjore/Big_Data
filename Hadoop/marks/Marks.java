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

public class Marks{
	public static class MarksMapper extends Mapper<Object, Text, Text, IntWritable>
	{
	
	 boolean isNumber(String s){ 
	for(int i=0; i<s.length(); i++){
		char ch = s.charAt(i);				
		if(ch < '0' || ch >'9')
			return false;
			}
			return true;	
		}
	 
	
	public void map(Object Key, Text value, Context context) throws IOException, 		InterruptedException{
	String line = value.toString();
	StringTokenizer st = new StringTokenizer(line, ",");
	String name = st.nextToken();
	int marks = Integer.parseInt(st.nextToken());
	String cls = st.nextToken();
		if(marks >= 60)
			context.write(new Text("Marks"), new IntWritable(marks));
		}
	}


	public static class MarksReducer extends Reducer<Text, IntWritable, Text, FloatWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
		FloatWritable avg = new FloatWritable();
		int sum = 0, total=0;
		for (IntWritable num : values)
		{
		sum = sum + num.get();
		total++;
		}
		}
		avg.set((float)sum/total);
		context.write(new Text("Average is"), avg);
		}
	   
public static void main(String args[]) throws Exception
{
	//create the object of Configyration class
	Configuration conf = new Configuration();
	
	
	// create the object of job class
	Job job = new Job(conf, "Marks");
	
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
	job.setMapperClass(MarksMapper.class);
	
	// Set the name of Reducer class
	job.setReducerClass(MarksReducer.class);
	// Set the input files path from 0th argument
	FileInputFormat.addInputPath(job, new Path(args[0]));
	
	// Set the output files path from 1st argument
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	// Execute the job and wait for completion
	job.waitForCompletion(true);
	}
}

	
	
	


