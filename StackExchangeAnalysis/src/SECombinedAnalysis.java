import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class SECombinedAnalysis {
	
	public static void main(String[] args) throws Exception { 
		Path in = new Path(args[0]);
		Path out1 = new Path(args[1]);
		Path out2 = new Path(args[2]);
		Path out3 = new Path(args[3]);
		Path out4 = new Path(args[4]);
			
		Configuration conf = new Configuration();
		
		ArrayList<Job> jobs = new ArrayList<>();
		jobs.add(AnalysisFindingFrequentlyUsedWords.generateAndConfigureJob(in, out1, conf));
		jobs.add(AnalysisOfQuestionsByDate.generateAndConfigureJob(in, out2, conf));
		jobs.add(AnalysisOfQuestionsJob.generateAndConfigureJob(in, out3, conf,"0"));
		jobs.add(AnalysisOfQuestionsJob.generateAndConfigureJob(in, out4, conf,"20"));

		JobControl jc = new JobControl("Questions Combined Analysis");
		
		for (Job job : jobs) {
			ControlledJob cj = new ControlledJob(conf);
			cj.setJob(job);
			jc.addJob(cj);
			
		}
		Thread t = new Thread(jc);
		t.setDaemon(true);
		t.start();

		while(!jc.allFinished()) {
			Thread.sleep(500);
		}
	}
}