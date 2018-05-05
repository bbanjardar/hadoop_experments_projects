package school.analysis.jobs;

//This is a generic map reduce outline class provided as part of the 
//Virtual Pair Programmers Hadoop For Java Developers training course.
//(www.virtualpairprogrammers.com). You may freely use this file for your
//own development.

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ExamScoresTopPerformers extends Configured implements Tool {

	public static class PostProcessingMap extends Mapper<Text, Text,Text, Text > {
		public void map (Text key, Text value, Context context)
					throws IOException, InterruptedException {
					context.write(new Text(key.toString().toUpperCase()),value);
			}
		}
	
	public static class MapClass extends Mapper<Text, Text,Text, Text > {

		public void map (Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] keys = key.toString().split(",");
			String newKey = keys[1];
			String newValue = keys[0] + "," + value;
			context.write(new Text(newKey),new Text(newValue));

		}
	}

	public static class Reduce extends Reducer<Text, Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//do reduce processing - write out the new key and value
			String college = "";
			Double highestScore = 0d;
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				String[] value = it.next().toString().split(",");  
				if (Double.valueOf(value[1]) > highestScore) {
					college = value[0];
					highestScore = Double.valueOf(value[1]);
				}
			}
			context.write(key, new Text(college + " - " + highestScore));

		}
	}


	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}

	public static Job generateAndConfigureJob(Path in ,Path out,Configuration conf ) throws Exception {
		deletePreviousOutput(conf, out);

		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		//this can be overridden at runtime by doing (for example):
		//-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=& 

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.setMapperClass(MapClass.class);

		ChainMapper.addMapper(job, MapClass.class, Text.class, Text.class, Text.class, Text.class, conf);
		//uncomment the following line if specifying a reducer 
		//job.setCombinerClass(Reduce.class);

		//job.setReducerClass(Reduce.class);
		ChainReducer.setReducer(job, Reduce.class, Text.class, Text.class, Text.class, Text.class, conf);
		ChainReducer.addMapper(job, PostProcessingMap.class, Text.class, Text.class, Text.class, Text.class, conf);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(ExamScoresTopPerformers.class);
		
		return job;
	}
	@Override
	public int run(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Configuration conf = this.getConf();
		Job job = generateAndConfigureJob(in, out, conf);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new ExamScoresTopPerformers(), args);
		System.exit(result);
	}
}