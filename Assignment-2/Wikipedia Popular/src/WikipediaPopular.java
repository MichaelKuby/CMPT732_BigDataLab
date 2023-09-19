import java.io.IOException;
import java.util.Iterator;

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

    public static class GetMaxReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long current_max_views = 0;
            Iterator<LongWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                long current_val = iterator.next().get();
                if (current_val > current_max_views) {
                    current_max_views = current_val;
                }
            }
            LongWritable maxViews = new LongWritable(current_max_views);
            context.write(key, maxViews);
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
        job.setCombinerClass(GetMaxReducer.class);
        job.setReducerClass(GetMaxReducer.class);

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