import java.net.URI;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;

public class SequenceFileWriter {

  private static String file2String(File file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));		
		
		String line = null;
		String result = "";
		while((line = reader.readLine()) != null){
			if(line.matches("[a-zA-Z]+")){		//过滤掉以数字开头或无意义的词，只选择英文单词
				result += line + " ";	
			}
		}
		reader.close();
		return result;
	}	
     //传入两个参数，args[0]为所读取的本地文件夹路径，而非HDFS文件路径;args[1]为想要写入文件的路径
    public static void main(String[] args) throws IOException {
        File[] dirs = new File(args[0]).listFiles();
      // 为创建SequenceFile.Writer准备参数
        String uri = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
                    value.getClass());
            for(File dir: dirs) {
                File[] files = dir.listFiles();
                for(File file: files) {
                    key.set(dir.getName());          // Key为类名,是CountryTrain数据集序列化的格式				
					//key.set(dir.getName() + ":" + file.getName());					//key为类名+":"+文件名，是CountryTest数据集序列化的格式	
                    value.set(file2String(file));	 // value为文件内容，是单词串
                    writer.append(key, value);
                }
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}