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
			if(line.matches("[a-zA-Z]+")){//���˵������ֿ�ͷ�Ĵ�
				result += line + " ";	
				//System.out.println(line);
			}
		}
		reader.close();
		return result;
	}	

    /**
     * ��������������
     * args[0]������ȡ���ļ���
     * args[1]����Ҫд���ļ�
     */
    public static void main(String[] args) throws IOException {
        File[] dirs = new File(args[0]).listFiles();

        // Ϊ����SequenceFile.Writer׼������
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
                    // �������
                    key.set(dir.getName());
                    // ֵ���ļ�����
                    value.set(file2String(file));
                    writer.append(key, value);
                }
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}