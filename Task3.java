import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  // add code here
  //A lot of accesses to DFS here. Can we do it with less? Does it matter?
  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>
  {
    private final static IntWritable zero = new IntWritable(0);
    private final static IntWritable one = new IntWritable(1);
    private IntWritable user = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] movieRatings = value.toString().split(",", 2);

      int index = 0;
      int columnNum = 1;
      while (index < movieRatings[1].length()) {
        user.set(columnNum);
        char rating = movieRatings[1].charAt(index);
        if (rating == ',') {
          context.write(user, zero);
          ++index;
          ++columnNum;
        }
        else {
          context.write(user, one);
          index += 2;
          ++columnNum;
        }
      }
    }
  }
  
  public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
  {
    //Key is user, values are whether they rated on each movie or not
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int ratings = 0;
      for (IntWritable rating : values) {
        ratings += rating.get();
      }
      context.write(key, new IntWritable(ratings));
    }
  }
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
