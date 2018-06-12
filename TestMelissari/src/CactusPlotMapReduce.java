import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CactusPlotMapReduce {
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		String[] myArgs = new GenericOptionsParser(args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "CactusPlotJob");
		
		String inputFile = myArgs[0];
		FileInputFormat.setInputPaths(job, new Path(inputFile));
		
		String outputFile = myArgs[1];
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
	}
}
