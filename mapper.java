package ryosuke;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public String FILENAME = "input/graph1.txt";
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split("\\s");
		for (int i=0; i<words.length; i++)
		{
			String word = words[i];
			if (word.length() > 0) {
				context.write(new Text(words[i]), new IntWritable(1));
			}
		}
		
		// hadoop以外の処理をここに記述
		try {
			FileReader in = new FileReader(FILENAME);
			BufferedReader br = new BufferedReader(in);
			String graphline;
			//System.out.println("1かいめ");
			while ((graphline = br.readLine()) != null) {
				System.out.println("\n");
				List<String> str = Arrays.asList(graphline.split("\t"));
				Iterator<String> i = str.iterator();
				String appendnodeinfo = "";
				int nodenum = 0;
				while(i.hasNext()){
					//int a = Integer.parseInt((String)i.next());
					String p = i.next();
					int colnum = Integer.parseInt(p);
					if(colnum == 1){
						//System.out.println(colnum + "debug");
						appendnodeinfo = appendnodeinfo + " " + returnline(nodenum);
						//System.out.println("appendnodeinfo: " + appendnodeinfo);
					}
					System.out.print(p);
					nodenum++;
				}
				System.out.println(appendnodeinfo);
			}
			br.close();
			in.close();
		} catch (IOException e) {
			System.out.println(e);
		}
	}
	
	// returnline: あるファイルの指定した行を返す
	public String returnline(int num){
		String line = "", appendinfo = "";
		try{
			FileReader in = new FileReader(FILENAME);
			BufferedReader br = new BufferedReader(in);
			//System.out.println("num: " + num + "in: " + in);
			for(int i = 0; i <= num; i++){
				if((line = br.readLine()) == null){
					break;
				}
				//System.out.println("line" + line);
			}
			br.close();
		} catch (IOException e){
			System.out.println(e);
		}
		
		// tabを取り除いて出力するものappendinfoを作る
		List<String> str = Arrays.asList(line.split("\t"));
		Iterator<String> ite = str.iterator();
		while(ite.hasNext()){
			appendinfo = appendinfo + ite.next();
		}
		return appendinfo;
	}
}