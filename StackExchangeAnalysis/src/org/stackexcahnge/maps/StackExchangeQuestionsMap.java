package org.stackexcahnge.maps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stackexcahnge.writable.SEWritable;

public class StackExchangeQuestionsMap extends Mapper<Text, Text, Text, SEWritable> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		SEWritable seValue = new SEWritable(value.toString());
		
		if(seValue.getPostType()==2) {
			key=new Text(String.valueOf(seValue.getParentId()));
		}
		context.write(key, seValue);
	}
}
