import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (!key.toString().equals("0")){
                String[] line = value.toString().split(",");
                context.write(new Text(line[10]), one);
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text val: values){
                result.set(val.toString());
                context.write(new Text(result+" "+key), NullWritable.get());
            }
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        Path tempPath = new Path("tmp-count" );
//        FileOutputFormat.setOutputPath(job, tempPath);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
System.out.print("ok1");
        if(!job.waitForCompletion(true)){
            Job sortjob = Job.getInstance(conf, "sort count");
            sortjob.setJarByClass(WordCount.class);
            FileInputFormat.addInputPath(sortjob, tempPath);
            sortjob.setInputFormatClass(SequenceFileInputFormat.class);
            sortjob.setMapperClass(InverseMapper.class);
            sortjob.setReducerClass(SortReducer.class);
            FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs.get(1)));
            sortjob.setOutputKeyClass(IntWritable.class);
            sortjob.setOutputValueClass(Text.class);

            sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            FileSystem.get(conf).deleteOnExit(tempPath);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}