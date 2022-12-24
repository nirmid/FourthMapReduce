import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import javafx.print.Collation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringTokenizer;


public class Main {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String w1 = itr.nextToken();
                String w2 = itr.nextToken();
                String w3 = itr.nextToken();
                String probability = itr.nextToken();
                word.set(w1+" "+w2);

                context.write(new Text(word),new Text(w3+","+probability));
            }
        }
    }
    public static class ParametersReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList<String[]> valuesTextArray = new ArrayList<>();
            for (Text value : values) {
                String[] split = value.toString().split(",");
                valuesTextArray.add(split);
            }
            valuesTextArray.sort(new Comparator<String[]>() {
                public int compare(String[] o1, String[] o2) {
                    Float num1 = Float.parseFloat(o1[1]);
                    Float num2 = Float.parseFloat(o2[1]);
                    return (int)(num1 - num2);
                }
            });
            for(String[] array: valuesTextArray){
                context.write(new Text(key+" "+array[0]),new Text(array[1]));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EMR4");
        job.setJarByClass(Main.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(ParametersReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //TODO
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        //FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}