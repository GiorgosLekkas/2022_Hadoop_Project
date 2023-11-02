import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Customers {
	
	IntWritable id = new IntWritable();
	IntWritable income = new IntWritable();
	Text date = new Text();
	IntWritable mnt_wines = new IntWritable();
	IntWritable mnt_fruits = new IntWritable();
	IntWritable mnt_meatproducts = new IntWritable();
	IntWritable mnt_fishproducts = new IntWritable();
	IntWritable mnt_sweetproducts = new IntWritable();
	IntWritable mnt_goldprods = new IntWritable();
	
	public Customers() {
		
	}
	
	public Customers(IntWritable id) {
		this.id = id;
	}
	
	public Customers(IntWritable id, IntWritable income, Text date, IntWritable mnt_wines, IntWritable mnt_fruits, IntWritable mnt_meatproducts, IntWritable mnt_fishproducts, IntWritable mnt_sweetproducts, IntWritable mnt_goldprods) {
		this.id = id;
		this.income = income;
		this.date = date;
		this.mnt_wines = mnt_wines;
		this.mnt_fruits = mnt_fruits;
		this.mnt_meatproducts = mnt_meatproducts;
		this.mnt_fishproducts = mnt_fishproducts;
		this.mnt_sweetproducts = mnt_sweetproducts;
		this.mnt_goldprods = mnt_goldprods;
	}
	
	public String getId() {
		return String.valueOf(id);
	}
	
	public String getIncome() {
		return String.valueOf(income);
	}
	
	public String getDate() {
		return date.toString();
	}
	
	public String getMntWines() {
		return String.valueOf(mnt_wines);
	}
	
	public String getMntFruits() {
		return String.valueOf(mnt_fruits);
	}
	
	public String getMntMeatproducts() {
		return String.valueOf(mnt_meatproducts);
	}
	
	public String getMntFishproducts() {
		return String.valueOf(mnt_fishproducts);
	}
	
	public String getMntSweetproducts() {
		return String.valueOf(mnt_sweetproducts);
	}
	
	public String getMntGoldprods() {
		return String.valueOf(mnt_goldprods);
	}
	
}
