import java.util.Comparator;

public class CompareCustomers implements Comparator<Customers>{
	
public int compare(Customers c1,Customers c2){
		
		if(c1.id.get()==c2.id.get()){
			return 0;
		}else if(c1.id.get()>c2.id.get()){
			return 1;  
		}else  
			return -1;
	}  

}
