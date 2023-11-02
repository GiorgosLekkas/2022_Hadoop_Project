/*
 * 		Lekkas Georgios 2022201800108 dit18108@go.uop.gr
 * 
 * 		Koundouropoulos Athanasios Symeon 2022201800097 dit18097@go.uop.gr
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;

public class WineBuy {

	public static class MyMapper extends Mapper <Object, Text, Text, Text> {

		Text textKey = new Text();
		int i = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
            Text sum = new Text("sum");
            Text c = new Text("c");
            String[] field = line.split(";");
            textKey.set(field[9]);
            IntWritable num = new IntWritable(0);
            if(textKey.toString().compareTo("MntWines")!=0) {
            	i++;
            	context.write(new Text(String.valueOf(i)), new Text(field[0] + "," + field[1] + "," + field[2] + "," + field[3] + "," + field[4] + "," + field[9] + ","));
            	context.write(c,new Text("1"));
                num = new IntWritable(Integer.parseInt(textKey.toString()));
                context.write(sum,new Text(String.valueOf(num)));
            }
        }
    } 

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
    	 Text textValue = new Text("");
		 int i = 0;

		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		 int sum = 0; 
    		 for (Text value : values) {
    			 if(key.toString().compareTo("c")==0||key.toString().compareTo("sum")==0)
    				 sum += Integer.valueOf(value.toString());
    			 else
    				 textValue = value;
    		 }
    		 if(key.toString().compareTo("c")==0||key.toString().compareTo("sum")==0)
    			 context.write(key, new Text(String.valueOf(sum)));
    		 else
    			 context.write(key, textValue);
    	 }
     }
    
    public static class Average extends Reducer<Text, Text, Text, Text> {
    	private FloatWritable result = new FloatWritable();
    	
		Text textKey = new Text();
		int i = 0;
		int j = 0;
		ArrayList <Customers> list = new ArrayList();
		
		IntWritable id = new IntWritable();
    	IntWritable age = new IntWritable();
		Text education = new Text();
		Text marital_status = new Text();
		IntWritable income = new IntWritable();
		IntWritable mnt_wine = new IntWritable();
		
    	float average = 0;
    	int count = 0;
    	int sum = 0;

    	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    		String str;
    		
    		for(Text value : values) {
	    		String line = value.toString();
	            String[] field = line.split(",");
	            if(key.toString().compareTo("c")!=0&&key.toString().compareTo("sum")!=0) {
	        		id = new IntWritable(Integer.parseInt(field[0]));
	        		age = new IntWritable(Integer.parseInt(field[1]));
	        		education.set(field[2]);
	        		marital_status = new Text(field[3]);
	        		mnt_wine = new IntWritable(Integer.parseInt(field[5]));
	        		if(field[4].compareTo("")!=0) {
	        			income = new IntWritable(Integer.parseInt(field[4]));
	        		}else
	        			income = new IntWritable(0);
	        		list.add(new Customers( id, age, education, marital_status, income, mnt_wine));
	        		
	        		Collections.sort(list,new CompareCustomers());
	            
	            }else {
	        		int num = Integer.valueOf(value.toString());
	        		if(key.toString().compareTo("c")==0) {
	        			count = num;
	        		}else if(key.toString().compareTo("sum")==0) {
	        			sum = num;
	        		}
	        	}
	        	if(count!=0&&sum!=0) {
	    	    	average = (float)sum/count;
	    	    	average += 0.5*average;
	    	    	result.set(average);
		        	for(Customers mntwines: list){  
		        		i++;
		        		str = mntwines.getId() + " " + mntwines.getAge() + " " + mntwines.getEducation() + " " + mntwines.getMaritalStatus() + " " + mntwines.getIncome() + " " + mntwines.getMntWine();
		        		FloatWritable fl = new FloatWritable(Float.parseFloat(mntwines.getMntWine()));
		        		if(fl.compareTo(result)>=0)
		        			context.write(new Text(String.valueOf(i)),new Text(str));
		        	}
	    	    }
    		}
    	}
    }
    
	public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Wine Buy");
	    job.setJarByClass(WineBuy.class);
	    job.setMapperClass(WineBuy.MyMapper.class);
	    job.setCombinerClass(WineBuy.MyReducer.class);
	    job.setReducerClass(WineBuy.Average.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}
