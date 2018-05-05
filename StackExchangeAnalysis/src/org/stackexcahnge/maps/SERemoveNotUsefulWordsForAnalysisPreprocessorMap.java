package org.stackexcahnge.maps;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.stackexcahnge.writable.SEWritable;

public class SERemoveNotUsefulWordsForAnalysisPreprocessorMap extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String[] commonNotUsefulWords = { "from", "have", "some", "that", "then", "this", "what", "when", "where" };
		String newValue = value.toString().toLowerCase();
		for (String word : commonNotUsefulWords) {
			newValue = newValue.replaceAll(word, "");
		}

		context.write(key, new Text(newValue));
	}
}