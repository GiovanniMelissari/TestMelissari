import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CactusPlotMapReduce {
	
	
	// =========== FIRST JOB ==================
	static class OrderByMapper extends Mapper<LongWritable, Text, CompositeKeyValue, DoubleWritable> {
		// hard-coded I know, but easiest :)
		private final int solver_index = 0;
		private final int status_index = 5;
		private final int time_index = 11;
		
		private final LongWritable zero = new LongWritable(0);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// ignore header file
			if(key.equals(zero))
				return;
			
			
			String[] cols = value.toString().split("\t");
			String status = cols[status_index];
			
			// ignore non-complete tests
			if(!status.equals("complete"))
				return;
			
			
			String solver = cols[solver_index];
			double time = Double.valueOf(cols[time_index]);
			
			// key: composite custom key that allows for custom sort, together with a Comparator class
			// value: the time for each instance solved by a solver
			context.write(new CompositeKeyValue(solver, time), new DoubleWritable(time));
		}
	
	}
	
	
	static class OrderByReducer extends Reducer<CompositeKeyValue, DoubleWritable, Text, Text> {
		
		private String previousSolver = new String();
		private long count = 0;
		
		@Override
		protected void reduce(CompositeKeyValue key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
		
			if(!key.getSolver().equals(previousSolver)) {
				previousSolver = key.getSolver();
				count = 0;
			}
			
			for(DoubleWritable row : values) {
				// key: solver
				// value: instance number - time
				context.write(new Text(key.getSolver()), new Text(count+"-"+String.valueOf(row.get())));
				count++;
			}
			
			// The instance number is calculated by incrementing a counter for each key received by the reducer.
			// Each time the reducer starts receiving a different key, the counter start from scratch.
			// This strategy is possible only because of my custom way of sorting keys
			// that allows me to group the solver tuples all together and make a secondary sort
			// by time. (Please see CompositeKeyValue.compareTo).
		}
		
	}
	
	
	
	
	
	
	
	
	
	
	
	// ==================== SECOND JOB =======================
	static class FormatMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		// Now is no more hard-coded because I decided my own file structure :)
		private final int solver_index = 0;

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] cols = value.toString().split("\t");
			
			String solver = cols[solver_index];
			
			for(int i=solver_index+1; i<cols.length; i++) {
				String[] splitted = cols[i].split("-");
				int instanceNum = Integer.valueOf(splitted[0]);
				double time = Double.valueOf(splitted[1]);
				
				// key: instance number
				// value: solver = time
				context.write(new LongWritable(instanceNum), new Text(solver + "=" + time));
			}
			
			
		}
		
	}
	
	
	
	static class FormatReducer extends Reducer<LongWritable, Text, Text, Text> {
		
		// Hard-coded.. again.. :/
		
		private final int num_of_solvers = 3;
		
		@SuppressWarnings("serial")
		private final HashMap<String, Integer> solvers = new HashMap<String, Integer>() {{
		    put("idlv+-s", 			0);
		    put("lp2normal+clasp", 	1);
		    put("me-asp", 			2);
		}};
		
		
		
		// With this member method I want to write the header of the final table
		// that I want it to be:
		//	Num_Inst	Solv1	Solv2	Solv3
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			StringBuffer stringBuffer = new StringBuffer();
			for(String solver : solvers.keySet())
				stringBuffer.append(solver+"\t");
				
			context.write(new Text("Num_Inst"), new Text(stringBuffer.toString()));
		}

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			// Little slight thing: I use array below to maintain the order of the
			// solvers equals to the one in our hash map (solv1, solv2, solv3).
			// Since the values in the reduce could potentially came up in any order,
			// it is important for the final result to put the right time value in the
			// right column. For that reason I used the hashmap with integers value,
			// so that I can maintain the order by index the array below.
			String[] times = new String[num_of_solvers];
			for(Text value : values) {
				String[] splitted = value.toString().split("=");
				String solver = splitted[0];
				String time = splitted[1];
				
				times[solvers.get(solver)] = time;
			}
			
			// Compose the 3 time values.
			StringBuffer stringBuffer = new StringBuffer();
			for(int i=0; i<times.length; i++)
				stringBuffer.append(times[i]+"\t");
			
			// key: instance number
			// value: time1 time2 time3
			context.write(new Text(key.toString()), new Text(stringBuffer.toString()));
		}
	}
	
	
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] myArgs = new GenericOptionsParser(args).getRemainingArgs();
		
		Job job1 = configFirstJob(conf, myArgs);
		boolean success = job1.waitForCompletion(true);
		
		if(success) {
			Job job2 = configSecondJob(conf, myArgs);
			success = job2.waitForCompletion(true);
		}
		
		System.exit(success ? 0 : 1);
	}










	private static Job configFirstJob(Configuration conf, String[] myArgs) throws IOException {
		Job job = Job.getInstance(conf, "OrderByJob");
		
		job.setMapperClass(OrderByMapper.class);
		job.setReducerClass(OrderByReducer.class);
		
		job.setMapOutputKeyClass(CompositeKeyValue.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setSortComparatorClass(TimeComparator.class);
		
		String inputFile = myArgs[0];
		FileInputFormat.setInputPaths(job, new Path(inputFile));
		
		String midFile = myArgs[1];
		FileOutputFormat.setOutputPath(job, new Path(midFile));
		return job;
	}

	private static Job configSecondJob(Configuration conf, String[] myArgs) throws IOException {
		Job job = Job.getInstance(conf, "FormatJob");
		
		job.setMapperClass(FormatMapper.class);
		job.setReducerClass(FormatReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String midFile = myArgs[1];
		FileInputFormat.setInputPaths(job, new Path(midFile));
		
		String outputFile = myArgs[2];
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		return job;
	}
}
