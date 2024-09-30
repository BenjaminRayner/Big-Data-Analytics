import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  // add code here
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
  {
    private Text name = new Text();
    private Text topRaters = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] movieRatings = value.toString().split(",", 2);

      char max = '1';
      int index = 0;
      int columnNum = 1;
      List<String> topColumns = new ArrayList<>();
      while (index < movieRatings[1].length()) {
        char rating = movieRatings[1].charAt(index);
        if (rating == ',') {
          ++index;
        }
        else {
          if (rating > max) {
            max = rating;
            topColumns.clear();
            topColumns.add(String.valueOf(columnNum));
          }
          else if (rating == max) {
            topColumns.add(String.valueOf(columnNum));
          }
          index += 2;
        }
        ++columnNum;
      }

      name.set(movieRatings[0]);
      topRaters.set(String.join(",", topColumns));
      context.write(name, topRaters);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here
    job.setMapperClass(TokenizerMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
