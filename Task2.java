import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {

  // add code here
  public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, IntWritable>
  {
    private IntWritable ratings = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] movieRatings = value.toString().split(",", 2);

      int numRatings = 0;
      int index = 0;
      while (index < movieRatings[1].length()) {
        char rating = movieRatings[1].charAt(index);
        if (rating == ',') {
          ++index;
        }
        else {
          ++numRatings;
          index += 2;
        }
      }

      ratings.set(numRatings);
      context.write(NullWritable.get(), ratings);
    }
  }
  
  public static class IntSumReducer extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable>
  {

    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int totalRatings = 0;
      for (IntWritable rating : values) {
        totalRatings += rating.get();
      }
      context.write(key, new IntWritable(totalRatings));
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task2");
    job.setJarByClass(Task2.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
