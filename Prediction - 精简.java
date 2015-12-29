import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

@SuppressWarnings("deprecation")
public class Prediction extends Configured implements Tool{
    //读取test文件,输入格式为：真实类名:文件名	单词 单词 ...... 
    public static class PredictionMap extends Mapper<Text, Text, Text, DoubleWritable> {	
    	private static Hashtable<String,Integer> toTct (File file) throws IOException{   //函数toTct获得(key:真实类名 单词,value:该单词在该类出现的次数)的哈希表
    		Hashtable<String,Integer> ht = new Hashtable<String,Integer>();
    		BufferedReader reader = new BufferedReader(new FileReader(file));		
    		Integer value;
    		String line = null;
    		String key = "";
    		while((line = reader.readLine()) != null){
    			StringTokenizer token = new StringTokenizer(line);
    			key=token.nextToken()+" "+token.nextToken();
    			value=Integer.parseInt(token.nextToken());
    			ht.put(key, value);
    		}
    		reader.close();
    		return ht;
    	}
    	private static Hashtable<String,Integer> toTtc (File file) throws IOException{  //函数toTtc获得(key:真实类名,value:该类中单词数)的哈希表
    		Hashtable<String,Integer> ht = new Hashtable<String,Integer>();
    		BufferedReader reader = new BufferedReader(new FileReader(file));		
    		Integer num;
    		Integer sum=0;
    		String line = null;
    		String classname = null;
    		while((line = reader.readLine()) != null){
    			StringTokenizer token = new StringTokenizer(line);
    			classname=token.nextToken();
    			token.nextToken();
    			num=Integer.parseInt(token.nextToken());
    			if(ht.containsKey(classname)){
    				sum=ht.get(classname)+num;
    				ht.put(classname, sum);
    			}
    			else{
    				ht.put(classname, num);
    			}
    		}
    		reader.close();
    		return ht;
    	}
    	private static Integer toB (File file) throws IOException{            //函数toB获得整个文档中的不同单词数
    		Hashtable<String,Integer> ht = new Hashtable<String,Integer>();
    		BufferedReader reader = new BufferedReader(new FileReader(file));		
    		Integer sum=0;
    		String line = null;
    		String word = "";
    		while((line = reader.readLine()) != null){
    			StringTokenizer token = new StringTokenizer(line);
    			token.nextToken();
    			word=token.nextToken();
    			if(!ht.containsKey(word)){
    				ht.put(word, 1);
    				++sum;
    			}
    		}
    		reader.close();
    		return sum;
    	}
    	Hashtable<String,Integer> Tct=new Hashtable<String,Integer>();
    	Hashtable<String,Integer> Ttc=new Hashtable<String,Integer>();
    	Integer B=0;
		Double Ttv=0.00000;     //Ttv:整个文档中的单词数
        public void setup(Context context) throws IOException,   InterruptedException{                
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());     
                       if (paths != null && paths.length > 0) {  				   
                             File file=new File(paths[0].toString());    
                              try {    
                          		Tct=toTct(file);                       
                            	Ttc=toTtc(file);
                            	B=toB(file);
                                Enumeration<Integer>  e = Ttc.elements();
                                while( e.hasMoreElements() ){
                                Ttv+=e.nextElement();
                                }
                              }    
                              catch (Exception e )  
                              {  
                                  e.printStackTrace();  
                              }      
                       }                      
        } 
		private final DoubleWritable probability = new DoubleWritable();	
		private Text word = new Text(); 
		private Text index = new Text(); 
		private Text classname = new Text();
		private Text keys = new Text();		
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Enumeration<String>  ee = Ttc.keys();
            while( ee.hasMoreElements() ){
            	classname.set(ee.nextElement());
            	keys.set(key+":"+classname);     //map输出的key的格式为：类名:文件名:测试的类名 
				String line = value.toString();  
                StringTokenizer token = new StringTokenizer(line);
            	while (token.hasMoreTokens()) {  
					word.set(token.nextToken()); 
					index.set(classname+" "+word);
					if(Tct.containsKey(index.toString())){
						probability.set((Tct.get(index.toString())+1.0)/(Ttc.get(classname.toString())+B));
						context.write(keys,probability);  //测试集中单词存在该类中的条件概率
					}
					else{
						probability.set((1.0)/(Ttc.get(classname.toString())+B));
						context.write(keys,probability);  //测试集中单词不存在该类中的条件概率（平滑处理）
					}
					}
				probability.set(Ttc.get(classname.toString())/Ttv);  //先验概率
				context.write(keys,probability); 		
            }
        }
    }
    public static class PredictionReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double C = 0.0;  
            for (DoubleWritable val : values) {
                C += Math.log(val.get());  
            }  
            context.write(key,new DoubleWritable(C));    //最后输出<真实类名:文件名:测试的类名 ,概率>
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  
		DistributedCache.addCacheFile(new Path("hdfs://hadoop:9000/user/root/CountryTrain/output/part-r-00000.txt").toUri(),conf);
		Job job = new Job(conf); 
        job.setJarByClass(Prediction.class);  	
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(DoubleWritable.class);  
        job.setMapperClass(PredictionMap.class);  
        job.setReducerClass(PredictionReduce.class);  
        job.setInputFormatClass(SequenceFileInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        FileInputFormat.addInputPath(job,new Path(args[0]));  
        FileOutputFormat.setOutputPath(job,new Path(args[1]));     
        job.waitForCompletion(true); 
    }
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}