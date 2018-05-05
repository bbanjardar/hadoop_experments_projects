
//This is a generic map reduce outline class provided as part of the 
//Virtual Pair Programmers Hadoop For Java Developers training course.
//(www.virtualpairprogrammers.com). You may freely use this file for your
//own development.

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.stackexcahnge.maps.StackExchangeQuestionsMap;
import org.stackexcahnge.reducers.StackExchange0ResponsesReducer;
import org.stackexcahnge.reducers.StackExchange20ResponsesReducer;
import org.stackexcahnge.util.SEUtility;
import org.stackexcahnge.writable.SEWritable;


public class AnalysisOfQuestionsJob  {

	public static Job generateAndConfigureJob( Path in, Path out,Configuration conf, String noOfQuestionsRequiement) throws IOException {
		int noOfResponses = Integer.parseInt(noOfQuestionsRequiement);
        
        SEUtility.deletePreviousOutput(conf, out);
		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "&");

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SEWritable.class);
		
		job.setMapperClass(StackExchangeQuestionsMap.class);

		//uncomment the following line if specifying a combiner
		//job.setCombinerClass(Combine.class);
		
		System.out.println("*****AnalysisOfQuestionsJob****main******noOfResponses******************* "+ noOfResponses);
		if(noOfResponses==20) {
			job.setReducerClass(StackExchange20ResponsesReducer.class);
		}else {
			job.setReducerClass(StackExchange0ResponsesReducer.class);
		}
		
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(AnalysisOfQuestionsJob.class);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        String noOfResponsesRequirement=args[2];
        Configuration conf = new Configuration();
        Job job = generateAndConfigureJob(in, out,conf ,noOfResponsesRequirement);
		job.submit();
		
	}
}
