import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Customers {
	
	IntWritable id = new IntWritable();
	IntWritable age = new IntWritable();
	Text education = new Text();
	Text marital_status = new Text();
	IntWritable income = new IntWritable();
	IntWritable mnt_wine = new IntWritable();
	

	public Customers() {
		
	}
	
	public Customers (IntWritable id, IntWritable age, Text education, Text marital_status, IntWritable income, IntWritable mnt_wine) {
		this.id = id;
		this.age = new IntWritable(2021 - age.get());
		this.education = education;
		this.marital_status = marital_status;
		this.income = income;
		this.mnt_wine = mnt_wine;
	}
	
	public String getId() {
		int a = id.get();
		return String.valueOf(a);
	}
	
	public String getAge() {
		int a = age.get();
		return String.valueOf(a);
	}
	
	public String getEducation() {
		return education.toString();
	}
	
	public String getMaritalStatus() {
		return marital_status.toString();
	}
	
	public String getIncome() {
		int a = income.get();
		return String.valueOf(a);
	}
	
	public String getMntWine() {
		int a = mnt_wine.get();
		return String.valueOf(a);
	}
}
