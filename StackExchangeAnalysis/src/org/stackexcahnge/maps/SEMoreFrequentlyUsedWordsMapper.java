package org.stackexcahnge.maps;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stackexcahnge.writable.SEWritable;

public class SEMoreFrequentlyUsedWordsMapper extends Mapper<Text, Text,Text, LongWritable > {

	public void map (Text key, Text value, Context context)
			throws IOException, InterruptedException {
		SEWritable seValue= new SEWritable(value.toString());
		//String text = seValue.getPostText().toLowerCase(); //not needed , as preprocessor is doing toLowerCase conversion
		String text = seValue.getPostText();
		
		String trimmedText = text.replaceAll("[^a-zA-Z\\s]", "");
	
		String[] words = trimmedText.split((" "));
		
		for (int i = 0; i < words.length -1; i++) {
			if (words[i].length() >=4 && words[i+1].length() >=4) {
				context.write(new Text(words[i]+" "+words[i+1]),new LongWritable(1));
			}
		}
	}
}