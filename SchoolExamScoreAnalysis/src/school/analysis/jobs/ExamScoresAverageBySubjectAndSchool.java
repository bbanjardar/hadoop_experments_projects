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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import school.analysis.jobs.ExamScoresTopPerformers.PostProcessingMap;


public class ExamScoresAverageBySubjectAndSchool extends Configured implements Tool {

	public static class PreProcessingMap extends Mapper<Text, Text,Text, Text > {
		public void map (Text key, Text value, Context context)
					throws IOException, InterruptedException {
				String[] values = value.toString().split(",");
				if(Long.valueOf(values[2])>=50) {
					context.write(key,value);
				}
			}
		}
	
	public static class MapClass extends Mapper<Text, Text,Text, LongWritable > {
		
		public void map (Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split(",");
			String newKey = values[0]+","+values[1];
			context.write(new Text(newKey),new LongWritable(Long.valueOf(values[2])));
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable,Text,Text> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			//do reduce processing - write out the new key and value
			Double sum = 0d;
			Long count = 0l;
			Iterator<LongWritable> it = values.iterator();
			while (it.hasNext()) {
				count++;
				sum += it.next().get();
			}
			context.write(key, new Text(String.valueOf(sum/count)));
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
	
	public static Job generateAndConfigureJob(Path in ,Path out,Configuration conf ,Configuration conf2) throws Exception {
		deletePreviousOutput(conf, out);
        
		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
        //conf.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        
        //this can be overridden at runtime by doing (for example):
        //-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=& 
        
		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//job.setMapperClass(MapClass.class);
		ChainMapper.addMapper(job, PreProcessingMap.class, Text.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, MapClass.class, Text.class, Text.class, Text.class, LongWritable.class, conf2);

		//uncomment the following line if specifying a reducer 
		//job.setCombinerClass(Reduce.class);
		ChainReducer.setReducer(job, Reduce.class, Text.class, LongWritable.class,Text.class,Text.class, conf2);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(ExamScoresAverageBySubjectAndSchool.class);
		return job;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        
        Configuration conf = this.getConf();
        Configuration conf2 = this.getConf();
		Job job = generateAndConfigureJob(in,out,conf,conf2);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new ExamScoresAverageBySubjectAndSchool(), args);
		System.exit(result);
	}
}
