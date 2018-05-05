package org.stackexcahnge.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/****

NOTE : will not be used as we will use LongSumReducer for this
  
***/
public class StackExchangeMoreFrequentlyUsedWordsReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		Iterator<LongWritable> it = values.iterator();
		Long wordsFreuencyCountSum=0L;
		
		while (it.hasNext()) {
			LongWritable frequencyCount = it.next();
			wordsFreuencyCountSum=wordsFreuencyCountSum+frequencyCount.get();
		}
		
		if(wordsFreuencyCountSum>=200) {
			context.write(new Text(key), new LongWritable(wordsFreuencyCountSum));
		}
	}
}
