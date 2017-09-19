package assets.a03;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class NginxLogParser {


    public static class LogMapper extends
    Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private static Logger logger = LoggerFactory.getLogger(LogMapper.class);
    private IntWritable hour = new IntWritable();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
          throws InterruptedException, IOException {
      logger.info("Mapper started");
      logger.info("key="+key);
      logger.info("value="+value);
      String line = ((Text) value).toString();
      // Matcher matcher = logPattern.matcher(line);
      // if (matcher.matches()) {
      //     String timestamp = matcher.group(4);
      //     try {
      //         hour.set(ParseLog.getHour(timestamp));
      //     } catch (ParseException e) {
      //         logger.warn("Exception", e);
      //     }
      //     context.write(hour, one);
      // }
      logger.info("Mapper Completed");
    }
  }


    
      // public static class LogMapper
      //      extends Mapper<Object, Text, Text, IntWritable>{
    
      //   static enum CountersEnum { INPUT_WORDS }
    
      //   private final static IntWritable one = new IntWritable(1);
      //   private Text word = new Text();
    
      //   private boolean caseSensitive;
      //   private Set<String> patternsToSkip = new HashSet<String>();
    
      //   private Configuration conf;
      //   private BufferedReader fis;
    
      //   @Override
      //   public void setup(Context context) throws IOException,
      //       InterruptedException {
      //     conf = context.getConfiguration();
      //   }
    
      //   @Override
      //   public void map(Object key, Text value, Context context
      //                   ) throws IOException, InterruptedException {
      //     System.out.println("key="+key);
      //     String line = value.toString();
      //     StringTokenizer itr = new StringTokenizer(line);
      //     while (itr.hasMoreTokens()) {
      //       word.set(itr.nextToken());
      //       context.write(word, one);
      //       Counter counter = context.getCounter(CountersEnum.class.getName(),
      //           CountersEnum.INPUT_WORDS.toString());
      //       counter.increment(1);
      //     }
      //   }
      // }
    
      public static class IntSumReducer
           extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
    
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
          int sum = 0;
          for (IntWritable val : values) {
            sum += val.get();
          }
          result.set(sum);
          context.write(key, result);
        }
      }
    
      public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
          System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
          System.exit(2);
        }
        Job job = Job.getInstance(conf, "Nginx Log Parser");
        job.setJarByClass(NginxLogParser.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    
        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
          otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
    }
    