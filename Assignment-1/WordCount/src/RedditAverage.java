import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;


import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, LongPairWritable>{

        private Text word = new Text();
        private final static Long one = Long.valueOf(1L);
        private LongPairWritable pair = new LongPairWritable();

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            JSONObject record = new JSONObject(value.toString());
            word.set(record.get("subreddit").toString());
            Long score = Long.valueOf(record.get("score").toString());
            pair.set(score, one);
            context.write(word, pair);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<LongPairWritable> pairs,
                           Context context
        ) throws IOException, InterruptedException {
            Double count = 0.0;
            Double sum_score = 0.0;
            for (LongPairWritable pair : pairs) {
                sum_score += pair.get_0();
                count += pair.get_1();
            }
            Double average = sum_score / count;
            result.set(average);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(RedditAverage.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPairWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
