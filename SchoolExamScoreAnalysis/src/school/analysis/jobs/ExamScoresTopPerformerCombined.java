package school.analysis.jobs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ExamScoresTopPerformerCombined {

	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}
	
	public static void main(String[] args) throws Exception { 
		Path in = new Path(args[0]);
		Path out1 = new Path(args[1]);
		Path out2 = new Path(args[2]);
			
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();

		conf1.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		//conf2.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job job1 = ExamScoresAverageBySubjectAndSchool.generateAndConfigureJob(in, out1, conf1, conf2);
		Job job2 = ExamScoresTopPerformers.generateAndConfigureJob(out1, out2, conf2);
	
		ControlledJob cj1 = new ControlledJob(conf1);
		cj1.setJob(job1);
		
		ControlledJob cj2 = new ControlledJob(conf2);
		cj2.setJob(job2);

		cj2.addDependingJob(cj1);

		JobControl jc = new JobControl("ExamScoresTopPerformerCombined");
		jc.addJob(cj1);
		jc.addJob(cj2);
		
		Thread t = new Thread(jc);
		t.setDaemon(true);
		t.start();

		while(!jc.allFinished()) {
			Thread.sleep(500);
		}
	}
}