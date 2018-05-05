package org.stackexcahnge.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.stackexcahnge.writable.SEWritable;

public class StackExchange0ResponsesReducer extends Reducer<Text, SEWritable,Text,Text> {
	
	public void reduce(Text key, Iterable<SEWritable> values, Context context)
			throws IOException, InterruptedException {

		Iterator<SEWritable> it = values.iterator();
		Long responsesCount=0L;
		while (it.hasNext()) {
			SEWritable post = it.next();
			if(post.getPostType()==2) {
				responsesCount=responsesCount+1;
			}
		}
		if(responsesCount==0) {
			context.write(new Text(key), new Text(""));
		}
	}
}
