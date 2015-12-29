import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

public class Evaluation {	
	static String[] ClassSet={                      //根据函数toTtc获得类名集合
			"ISRAEL","CANA","MEX","EEC","FRA","UK","HKONG","JAP","ARG","GFR","BELG","SAFR","BRAZ","ALB","MALAY","ITALY","INDON","POL",
			"AUSTR","SKOREA","CHINA","SINGP","RUSS","THAIL","INDIA","NETH","CZREP","PHLNS","FIN"};
	static int ClassSum=ClassSet.length;    //类的总数
	private static Hashtable<String,String> predictionClass(File file) throws IOException{     //函数predictionClass获得(key:真实类名:文件名,value:预测类名)的哈希表
		Hashtable<String,String> ht = new Hashtable<String,String>();
		Hashtable<String,Double> cache = new Hashtable<String,Double>();
		BufferedReader reader = new BufferedReader(new FileReader(file));		
		Double value;
		Double max=-Double.MAX_VALUE;
		String line = null;
		String key = null;
		String a = null;
		Integer index=ClassSum;
		while((line = reader.readLine()) != null&&(index--) != 0){			
			StringTokenizer token = new StringTokenizer(line);
			key=token.nextToken();
			value=Double.parseDouble(token.nextToken());
			if(value>max){
				max=value;
			}
			cache.put(key, value);
			if(index==0) {
				Enumeration<String>  e = cache.keys();
                while( e.hasMoreElements() ){
                a=e.nextElement();
                if(max==cache.get(a)){
        			String[] part =a.split(":");	
        			ht.put(part[0]+":"+part[1],part[2]);
                }
                }
				index=ClassSum;
				max=-Double.MAX_VALUE;
				cache.clear();
			}
		}
		reader.close();
		return ht;
	}
	public static void main(String[] args) throws Exception {
		String a=null;
		String b=null;
		int index=0,f;
		double TP=0.0,FN=0.0,FP=0.0,TN=0.0;
		double P,R,F;
		double Precision[]=new double[ClassSum];
		double Recall[]=new double[ClassSum];
		double F1[]=new double[ClassSum];
		double sumuP=0.0,sumR=0.0,sumF=0.0;
		Hashtable<String,String> pc = new Hashtable<String,String>();
		File file=new File(args[0].toString());   
        pc=predictionClass(file);            
        for(String c: ClassSet){  
            Enumeration<String>  e = pc.keys();
            while( e.hasMoreElements() ){
             a=e.nextElement();
             String[] part =a.split(":");
             b=pc.get(a);
             if((c.equals(part[0]))&&c.equals(b)) TP++;   
             else if(c.equals(part[0])&&!c.equals(b)) FN++;
             else if(!c.equals(part[0])&&c.equals(b)) FP++;
             else if(!c.equals(part[0])&&!c.equals(b))  TN++;
            }
            P=TP/(TP+FP);  //Precision
            R=TP/(TP+FN);   //Recall
            F=2*P*R/(P+R);   //F1
            f=index++;
            Precision[f]=P;
    		Recall[f]=R;
    		F1[f]=F; 
    		TP=0.0;FN=0.0;FP=0.0;TN=0.0;
        }
        for(index=0;index<ClassSum;index++){
    		sumuP+=Precision[index];
    		sumR+=Recall[index];
    		sumF+=F1[index];
        }
		System.out.println("平均Precision精度值："+sumuP/ClassSum);
        System.out.println("平均Recall精度值："+sumR/ClassSum);
        System.out.println("平均的调和均值："+sumF/ClassSum);
	}
}
