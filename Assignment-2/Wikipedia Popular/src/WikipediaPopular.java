import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class WikipediaPopular extends Configured implements Tool {

    public static class PageViewMapper
            extends Mapper<LongWritable, Text, Text, LongWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(" "); // Split on the spaces

            // Check the input length to make sure it makes sense
            if (tokens.length == 5) {
                if (tokens[1].equals("en") && !(tokens[2].equals("Main_Page") || tokens[2].startsWith("Special:"))) {
                    Text datetime = new Text(tokens[0]);
                    LongWritable views = new LongWritable (Long.parseLong(tokens[3])); // Convert from String to Long
                    context.write(datetime, views);
                }
            }
        }
    }

//    public static class IntSumReducer
//            extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
//        private DoubleWritable result = new DoubleWritable();
//
//        @Override
//        public void reduce(Text key, Iterable<LongPairWritable> pairs,
//                           Context context
//        ) throws IOException, InterruptedException {
//            double count = 0.0;
//            double sum_score = 0.0;
//            for (LongPairWritable pair : pairs) {
//                sum_score += pair.get_0();
//                count += pair.get_1();
//            }
//            double average = sum_score / count;
//            result.set(average);
//            context.write(key, result);
//        }
//    }
//
    public static class GetMaxCombiner
        extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            LongWritable current_max_views = new LongWritable();
            for (LongWritable val : values) {
                if (val.get() > current_max_views.get()) {
                    current_max_views = this_views;
                }
            }

            context.write(key, current_max_views);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Wikipedia Popular Pages");
        job.setJarByClass(WikipediaPopular.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(PageViewMapper.class);
        //job.setCombinerClass(SumScoreCombiner.class);
        //job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}