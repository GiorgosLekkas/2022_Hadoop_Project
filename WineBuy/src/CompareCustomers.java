import java.util.Comparator;

public class CompareCustomers  implements Comparator<Customers> {
	
	public int compare(Customers m1,Customers m2){
		
		if(m1.mnt_wine.get()==m2.mnt_wine.get()){
			if(m1.income.get()==m2.income.get()){
				return 0;
			}else if(m1.income.get()>m2.income.get()){
				return -1;  
			}else 
				return 1;
		}else if(m1.mnt_wine.get()>m2.mnt_wine.get()){
			return -1;  
		}else  
			return 1;
	}  

}
