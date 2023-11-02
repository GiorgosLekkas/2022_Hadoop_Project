/*
 * 		Lekkas Georgios 2022201800108 dit18108@go.uop.gr
 * 
 * 		Koundouropoulos Athanasios Symeon 2022201800097 dit18097@go.uop.gr
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataEdit {

	public static class MyMapper extends Mapper <Object, Text, Text, Text> {

		Text textKey = new Text();
		int i = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
            Text c = new Text("c");
            String[] field = line.split(";");
            if(field[4].compareTo("")==0) {
            	field[4] = "0";
            }
            String temp = new String();
            for(int i = 0;i<field.length;i++) {
            	if(i!=field.length-1) {
            		temp = temp.concat(field[i] + ";");
            	}else
            		temp = temp.concat(field[i]);
            }
            context.write(new Text(temp), new Text());
		}
	} 
    
	public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Wine Buy");
	    job.setJarByClass(DataEdit.class);
	    job.setMapperClass(DataEdit.MyMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}

