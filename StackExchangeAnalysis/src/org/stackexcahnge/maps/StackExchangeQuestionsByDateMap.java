package org.stackexcahnge.maps;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stackexcahnge.writable.SEWritable;

public class StackExchangeQuestionsByDateMap extends Mapper<Text, Text, Text, LongWritable> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		SEWritable seValue = new SEWritable(value.toString());
		
		if(seValue.getPostType()==1) {
			key=new Text(seValue.getDateCreatedUpdated().substring(0,7));
			context.write(key, new LongWritable(1));
		}
	}
}
