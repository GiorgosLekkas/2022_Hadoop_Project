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
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ranking {

	public static class MyMapper extends Mapper <Object, Text, Text, Text> {

		Text textKey = new Text();
		int i = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
        	Text sum_income = new Text("sum_income");
            Text sum_wines = new Text("sum_wines");
            Text sum_fruits = new Text("sum_fruits");
            Text sum_meatproducts = new Text("sum_meatproducts");
            Text sum_fishproducts = new Text("sum_fishproducts");
            Text sum_sweetproducts = new Text("sum_sweetproducts");
            Text sum_goldprods = new Text("sum_goldprods");
            Text c = new Text("c");
            String[] field = line.split(";");
            textKey.set(field[9]);
            IntWritable num = new IntWritable(0);
            if(textKey.toString().compareTo("MntWines")!=0) {
            	i++;
            	context.write(new Text(String.valueOf(i)), new Text(field[0] + "," + field[4] + "," + field[7] + "," + field[9] + "," + field[10] + "," + field[11] + "," + field[12] + "," + field[13] + "," + field[14] + ","));
            	context.write(c,new Text("1"));
                num = new IntWritable(Integer.parseInt(textKey.toString()));
                if(field[4].compareTo("")!=0) {
                	context.write(sum_income,new Text(String.valueOf(field[4])));
                }else
                	context.write(sum_income,new Text("0"));
                context.write(sum_wines,new Text(String.valueOf(field[9])));
                context.write(sum_fruits,new Text(String.valueOf(field[10])));
                context.write(sum_meatproducts,new Text(String.valueOf(field[11])));
                context.write(sum_fishproducts,new Text(String.valueOf(field[12])));
                context.write(sum_sweetproducts,new Text(String.valueOf(field[13])));
                context.write(sum_goldprods,new Text(String.valueOf(field[14])));
            }
        }
    } 

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
    	 Text textValue = new Text("");
    	 String k;
		 int i = 0;

		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		 int sum = 0; 
    		 for (Text value : values) {
    			 k = key.toString();
    			 if(k.compareTo("c")==0||k.compareTo("sum_income")==0||k.compareTo("sum_wines")==0||k.compareTo("sum_fruits")==0||k.compareTo("sum_meatproducts")==0||k.compareTo("sum_fishproducts")==0||k.compareTo("sum_sweetproducts")==0||k.compareTo("sum_goldprods")==0)
    				 sum += Integer.valueOf(value.toString());
    			 else
    				 textValue = value;
    		 }
    		 if(k.compareTo("c")==0||k.compareTo("sum_income")==0||k.compareTo("sum_wines")==0||k.compareTo("sum_fruits")==0||k.compareTo("sum_meatproducts")==0||k.compareTo("sum_fishproducts")==0||k.compareTo("sum_sweetproducts")==0||k.compareTo("sum_goldprods")==0)
    			 context.write(key, new Text(String.valueOf(sum)));
    		 else
    			 context.write(key, textValue);
    	 }
     }
    
    public static class Average extends Reducer<Text, Text, Text, Text> {
    	   	
		Text textKey = new Text();
		int i = 0;
		int j = 0;
		
		ArrayList <Customers> list = new ArrayList();
		ArrayList <Customers> gold = new ArrayList();
		ArrayList <Customers> silver = new ArrayList();
		ArrayList <Customers> bronze = new ArrayList();
		ArrayList <Customers> papper = new ArrayList();
		
		IntWritable id = new IntWritable();
		IntWritable income = new IntWritable();
		Text date = new Text();
		IntWritable mnt_wines = new IntWritable();
		IntWritable mnt_fruits = new IntWritable();
		IntWritable mnt_meatproducts = new IntWritable();
		IntWritable mnt_fishproducts = new IntWritable();
		IntWritable mnt_sweetproducts = new IntWritable();
		IntWritable mnt_goldprods = new IntWritable();
		
    	float average = 0;
    	float[] gs = new float[7];
    	float[] bp = new float[7];
    	
    	int count = 0;
    	int sum_income = 0;
    	int sum_wines = 0;
    	int sum_fruits = 0;
    	int sum_meatproducts = 0;
    	int sum_fishproducts = 0;
    	int sum_sweetproducts = 0;
    	int sum_goldprods = 0;

    	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    		for(Text value : values) {
	    		String line = value.toString();
	            String[] field = line.split(",");
	            String k = key.toString();
	            
	            if(k.compareTo("c")!=0&&k.compareTo("sum_income")!=0&&k.compareTo("sum_wines")!=0&&k.compareTo("sum_fruits")!=0&&k.compareTo("sum_meatproducts")!=0&&k.compareTo("sum_fishproducts")!=0&&k.compareTo("sum_sweetproducts")!=0&&k.compareTo("sum_goldprods")!=0) {
	            	id = new IntWritable(Integer.parseInt(field[0]));
	            	if(field[1].compareTo("")!=0)
	            		income = new IntWritable(Integer.parseInt(field[1]));
	            	else
	            		income = new IntWritable(0);
	            	date = new Text(field[2]);
	            	mnt_wines = new IntWritable(Integer.parseInt(field[3]));
	            	mnt_fruits = new IntWritable(Integer.parseInt(field[4]));
	            	mnt_meatproducts = new IntWritable(Integer.parseInt(field[5]));
	            	mnt_fishproducts = new IntWritable(Integer.parseInt(field[6]));
	            	mnt_sweetproducts = new IntWritable(Integer.parseInt(field[7]));
	            	mnt_goldprods = new IntWritable(Integer.parseInt(field[8]));
	            	list.add(new Customers( id, income, date, mnt_wines, mnt_fruits, mnt_meatproducts, mnt_fishproducts, mnt_sweetproducts, mnt_goldprods));
	            	
	            	Collections.sort(list,new CompareCustomers());
	            }else {
	        		int num = Integer.valueOf(value.toString());
	        		if(key.toString().compareTo("c")==0) {
	        			count = num;
	        		}else if(key.toString().compareTo("sum_income")==0) {
	        			sum_income = num;
	        		}else if(key.toString().compareTo("sum_wines")==0) {
	        			sum_wines = num;
	        		}else if(key.toString().compareTo("sum_fruits")==0) {
	        			sum_fruits = num;
	        		}else if(key.toString().compareTo("sum_meatproducts")==0) {
	        			sum_meatproducts = num;
	        		}else if(key.toString().compareTo("sum_fishproducts")==0) {
	        			sum_fishproducts = num;
	        		}else if(key.toString().compareTo("sum_sweetproducts")==0) {
	        			sum_sweetproducts = num;
	        		}else if(key.toString().compareTo("sum_goldprods")==0) {
	        			sum_goldprods = num;
	        		}
	        		
	        	}
	            if(count!=0) {
	            	if(sum_income!=0) {
		    	    	average = (float)sum_income/count;
		    	    	gs[0] = average;
		    	    	bp[0] = average;
		    	    }
	            	if(sum_wines!=0) {
		    	    	average = (float)sum_wines/count;
		    	    	gs[1] = (float) (average + average*0.5);
		    	    	bp[1] = (float) (average*0.25);
		    	    }
	            	if(sum_fruits!=0) {
		    	    	average = (float)sum_fruits/count;
		    	    	gs[2] = (float) (average + average*0.5);
		    	    	bp[2] = (float) (average*0.25);
		    	    }
	            	if(sum_meatproducts!=0) {
		    	    	average = (float)sum_meatproducts/count;
		    	    	gs[3] = (float) (average + average*0.5);
		    	    	bp[3] = (float) (average*0.25);
		    	    }
	            	if(sum_fishproducts!=0) {
		    	    	average = (float)sum_fishproducts/count;
		    	    	gs[4] = (float) (average + average*0.5);
		    	    	bp[4] = (float) (average*0.25);
		    	    }
	            	if(sum_sweetproducts!=0) {
		    	    	average = (float)sum_sweetproducts/count;
		    	    	gs[5] = (float) (average + average*0.5);
		    	    	bp[5] = (float) (average*0.25);
		    	    }
	            	if(sum_goldprods!=0) {
		    	    	average = (float)sum_goldprods/count;
		    	    	gs[6] = (float) (average + average*0.5);
		    	    	bp[6] = (float) (average*0.25);
		    	    }
	            }
	            

    		}
    		if(count!=0&&sum_fishproducts!=0&&sum_fruits!=0&&sum_goldprods!=0&&sum_income!=0&&sum_meatproducts!=0&&sum_sweetproducts!=0&&sum_wines!=0) {
    			
    			for(Customers cust: list) {
    				String l = cust.getDate();
    				String d = l + "/";
    	            String[] date = l.split("/");
    	            
    	            int year = Integer.parseInt(date[2]);
    	            
    	            IntWritable gold_id;
    	            IntWritable silver_id;
    	            IntWritable bronze_id;
    	            IntWritable papper_id;
    	            
    	            float[] arr = {Float.parseFloat(cust.getIncome()),Float.parseFloat(cust.getMntWines()),Float.parseFloat(cust.getMntFruits()),Float.parseFloat(cust.getMntMeatproducts()),Float.parseFloat(cust.getMntFishproducts()),Float.parseFloat(cust.getMntSweetproducts()),Float.parseFloat(cust.getMntGoldprods())};
    	            
    	            if(year==21&&arr[0]>69500&&gs[1]<arr[1]&&gs[2]<arr[2]&&gs[3]<arr[3]&&gs[4]<arr[4]&&gs[5]<arr[5]&&gs[6]<arr[6]) {
    	            	gold_id = new IntWritable(Integer.parseInt(cust.getId()));
    	            	gold.add(new Customers(gold_id));
    	            }else if(year<21&&arr[0]>69500&&gs[1]<arr[1]&&gs[2]<arr[2]&&gs[3]<arr[3]&&gs[4]<arr[4]&&gs[5]<arr[5]&&gs[6]<arr[6]) {
    	            	silver_id = new IntWritable(Integer.parseInt(cust.getId()));
    	            	silver.add(new Customers(silver_id));
    	            }else if(year==21&&bp[0]>arr[0]&&bp[1]>arr[1]&&bp[2]>arr[2]&&bp[3]>arr[3]&&bp[4]>arr[4]&&bp[5]>arr[5]&&bp[6]>arr[6]) {
    	            	bronze_id = new IntWritable(Integer.parseInt(cust.getId()));
    	            	bronze.add(new Customers(bronze_id));
    	            }else if(year<21&&bp[0]>arr[0]&&bp[1]>arr[1]&&bp[2]>arr[2]&&bp[3]>arr[3]&&bp[4]>arr[4]&&bp[5]>arr[5]&&bp[6]>arr[6]) {
    	            	papper_id = new IntWritable(Integer.parseInt(cust.getId()));
    	            	papper.add(new Customers(papper_id));
    	            }
    				
    			}
    			String str1 = "", str2 = "", str3 = "", str4 = "";
    			for(Customers cust: gold) {
    				str1 = str1.concat(cust.getId() + " ");
    			}
    			for(Customers cust: silver) {
    				str2 = str2.concat(cust.getId() + " ");
    			}
    			for(Customers cust: bronze) {
    				str3 = str3.concat(cust.getId() + " ");
    			}
    			for(Customers cust: papper) {
    				str4 = str4.concat(cust.getId() + " ");
    			}
    			context.write(new Text("Gold"),new Text(str1));
    			context.write(new Text("Silver"),new Text(str2));
    		}
    	}
    }
    
	public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Wine Buy");
	    job.setJarByClass(Ranking.class);
	    job.setMapperClass(Ranking.MyMapper.class);
	    job.setCombinerClass(Ranking.MyReducer.class);
	    job.setReducerClass(Ranking.Average.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}

