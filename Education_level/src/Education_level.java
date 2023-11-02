/*
 * 		Lekkas Georgios 2022201800108 dit18108@go.uop.gr
 * 
 * 		Koundouropoulos Athanasios Symeon 2022201800097 dit18097@go.uop.gr
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Education_level {

    public static class mymapper extends Mapper <Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        Text textKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                      String line = value.toString();
                      String[] field = line.split(";");
                      textKey.set(field[2]);
                      if(textKey.toString().compareTo("Education")!=0) {
                    	  context.write(textKey,one);
                      }
            }
    } 

    public static class myreducer extends Reducer<Text, IntWritable, Text, IntWritable> {
         Text textValue = new Text();

         public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
               int sum = 0; 
               for (IntWritable value : values) {

                    sum += value.get();
                }
               context.write(key, new IntWritable(sum));
         }
    }


	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "best athletes");
	    job.setJarByClass(Education_level.class);
	    job.setMapperClass(Education_level.mymapper.class);
	    //job.setCombinerClass(Main.myreducer.class);
	    job.setReducerClass(Education_level.myreducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
