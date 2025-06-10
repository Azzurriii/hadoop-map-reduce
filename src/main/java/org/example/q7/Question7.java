import java.io.IOException;
import java.util.StringTokenizer;

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

public class Question7 {

    public static class HistogramMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable pixelValue = new IntWritable();
        private IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // Split line by spaces or tabs to get individual pixels
            StringTokenizer tokenizer = new StringTokenizer(line, " \t");

            while (tokenizer.hasMoreTokens()) {
                try {
                    String token = tokenizer.nextToken().trim();
                    if (!token.isEmpty()) {
                        int pixel = Integer.parseInt(token);
                        pixelValue.set(pixel);
                        context.write(pixelValue, one);
                    }
                } catch (NumberFormatException e) {
                    // Skip invalid pixels
                }
            }
        }
    }

    public static class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable count = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            // Count all occurrences of this pixel value
            for (IntWritable value : values) {
                sum += value.get();
            }

            count.set(sum);
            context.write(key, count);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Question7 <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "image histogram");
        job.setJarByClass(Question7.class);

        job.setMapperClass(HistogramMapper.class);
        job.setCombinerClass(HistogramReducer.class);
        job.setReducerClass(HistogramReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}