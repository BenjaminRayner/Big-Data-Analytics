import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {

  // add code here
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
  {
    private HashMap<String,Integer> movieLineMap = new HashMap<>();
    private List<String> moviesCached = new ArrayList<>();
    private List<String> ratingsCached = new ArrayList<>();
    private int startLine = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      //Get whole file cached
      Configuration conf = context.getConfiguration();
      URI fileURI = Job.getInstance(conf).getCacheFiles()[0];
      String fileName = new Path(fileURI.getPath()).getName().toString();
      BufferedReader fis = new BufferedReader(new FileReader(fileName));

      String line = null;
      for (int lineNum = 0; (line = fis.readLine()) != null; ++lineNum) {
        String[] movieRatingsSplit = line.split(",", 2);
        movieLineMap.put(movieRatingsSplit[0], lineNum);
        moviesCached.add(movieRatingsSplit[0]);
        ratingsCached.add(movieRatingsSplit[1]);
      }
      fis.close();
    }

    private Text moviePair = new Text();
    private IntWritable numSimilarities = new IntWritable();

    //Takes lines for specific chunk of file
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      //Can we avoid split?
      String[] movieRatingsSplitLocal = value.toString().split(",", 2);
      String movieLocal = movieRatingsSplitLocal[0];
      String ratingsLocal = movieRatingsSplitLocal[1];

      //Find the line chunk starts on
      if (startLine == 0) {
        startLine = movieLineMap.get(movieLocal);
      }
      ++startLine;

      for (int lineNum = startLine; lineNum < moviesCached.size(); ++lineNum) {
        int localIndex = 0;
        int cacheIndex = 0;
        int similarities = 0;
        while ((localIndex < ratingsLocal.length()) && (cacheIndex < ratingsCached.get(lineNum).length())) {
          if (ratingsLocal.charAt(localIndex) == ',') ++localIndex;
          else {
            if (ratingsLocal.charAt(localIndex) == ratingsCached.get(lineNum).charAt(cacheIndex)) ++similarities;
            localIndex += 2;
          }
          if (ratingsCached.get(lineNum).charAt(cacheIndex) == ',') ++cacheIndex;
          else cacheIndex += 2;
        }

        //Order lexicographically
        if (movieLocal.compareTo(moviesCached.get(lineNum)) < 0)
          moviePair.set(movieLocal + "," + moviesCached.get(lineNum));
        else
          moviePair.set(moviesCached.get(lineNum) + "," + movieLocal);
        numSimilarities.set(similarities);
        context.write(moviePair, numSimilarities);
      }
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(TokenizerMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.addCacheFile(new Path(otherArgs[0]).toUri());

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
