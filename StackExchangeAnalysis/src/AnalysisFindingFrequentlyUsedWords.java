
//This is a generic map reduce outline class provided as part of the 
//Virtual Pair Programmers Hadoop For Java Developers training course.
//(www.virtualpairprogrammers.com). You may freely use this file for your
//own development.


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.stackexcahnge.maps.SEDiscardWordsWithlessThan200CountPostProcessorMap;
import org.stackexcahnge.maps.SEMoreFrequentlyUsedWordsMapper;
import org.stackexcahnge.maps.SERemoveNotUsefulWordsForAnalysisPreprocessorMap;
import org.stackexcahnge.util.SEUtility;


public class AnalysisFindingFrequentlyUsedWords  {

	public static Job generateAndConfigureJob(Path in, Path out, Configuration conf) throws IOException {
		
		SEUtility.deletePreviousOutput(conf, out);
		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "&");

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//job.setMapperClass(SEMoreFrequentlyUsedWordsMapper.class); 
		ChainMapper.addMapper(job, SERemoveNotUsefulWordsForAnalysisPreprocessorMap.class, Text.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, SEMoreFrequentlyUsedWordsMapper.class, Text.class, Text.class, Text.class, LongWritable.class, conf);

		ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);
		ChainReducer.addMapper(job, SEDiscardWordsWithlessThan200CountPostProcessorMap.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);
		//uncomment the following line if specifying a combiner
		//job.setCombinerClass(Combine.class);
		
		System.out.println("*****AnalysisFindingFrequentlyUsedWords****main************************ ");
		//job.setReducerClass(StackExchangeMoreFrequentlyUsedWordsReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(AnalysisFindingFrequentlyUsedWords.class);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        
        Configuration conf = new Configuration();
        Job job = generateAndConfigureJob(in, out, conf);
		job.submit();
		
	}
}
