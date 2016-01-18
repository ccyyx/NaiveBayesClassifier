import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class IndustryTrain {	
    //读取train文件,输入格式为：类名	单词 单词 ...... 
    public static class IndustryTrainMap extends Mapper<Text, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);         
		private Text class_word = new Text();	
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {  			
			String line = value.toString();  
            StringTokenizer token = new StringTokenizer(line);  
            while (token.hasMoreTokens()) {                 
				class_word.set(key +" "+ token.nextToken());
                context.write(class_word,one);  
            } 
        }
    }
	public static class IndustryTrainReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;  
			for (IntWritable val : values) {  
				sum += val.get();  
			}  
			context.write(key,new IntWritable(sum));  //最后输出<<类名 单词>,单词总数>
		}
	}
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(IndustryTrain.class); 		
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);   
        job.setMapperClass(IndustryTrainMap.class);  
        job.setReducerClass(IndustryTrainReduce.class);   
        job.setInputFormatClass(SequenceFileInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);   
        FileInputFormat.addInputPath(job,new Path(args[0]));  
        FileOutputFormat.setOutputPath(job,new Path(args[1]));   
        job.waitForCompletion(true); 
    }
}