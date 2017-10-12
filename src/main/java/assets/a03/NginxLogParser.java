package assets.a03;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NginxLogParser {


    public static class LogMapper extends
    Mapper<LongWritable,Text, Text, IntWritable> {

    private static Logger logger = LoggerFactory.getLogger(LogMapper.class);
    private IntWritable hour = new IntWritable();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
          throws InterruptedException, IOException {
      logger.info("Mapper started");
      logger.info("key="+key);
      logger.info("value="+value);
      String line = ((Text) value).toString();
      
      String logEntryPattern = "^(\\S+) (\\S+) .*";
      Pattern p = Pattern.compile(logEntryPattern);
      Matcher matcher = p.matcher(value.toString());
      if (matcher.matches()) {
          Text s_ip_text = new Text();
          String s_ip = matcher.group(1);
          String t_ip = matcher.group(2);
          logger.info("s_ip="+s_ip);
          logger.info("t_ip="+t_ip);
          s_ip_text.set(s_ip);
          context.write(s_ip_text, one);
      }

      logger.info("Mapper Completed");
    }
  }
    
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
        System.out.println(remainingArgs.length); 
        if (remainingArgs.length != 2 ) {
          System.err.println("Usage: NginxLogParser <in> <out>");
          System.exit(1);
        }
        Job job = Job.getInstance(conf, "Nginx Log Parser " + String.valueOf((Math.random()*10000)));
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
    