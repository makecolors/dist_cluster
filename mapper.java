package ryosuke;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
	public String FILENAME = "input/graph4.txt";
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// hadoop以外の処理をここに記述
		if(value == null){
			System.out.println("no value");
			System.exit(-1);
		}
		try{
			String line = value.toString();
			String[] splitcolon = line.split(":");
			String nodenum = splitcolon[0];
			//System.out.println("nodenum: " + nodenum);
			
			// reduce処理に渡すためのappendnodelistを作成する
			String edgeset = splitcolon[1];
			List<String> edge = Arrays.asList(edgeset.split("\t"));
			Iterator<String> i = edge.iterator();
			ArrayList<String> appendnodelist = new ArrayList<String>();
			while(i.hasNext()){
				String[] splitcamma = i.next().split(",");
				appendnodelist.add(splitcamma[1]);
			}
			//System.out.println("appendlist: " + appendnodelist.toString());
			//System.out.println(Arrays.toString(edge));
			//System.out.println(edgeset);
			
			// 渡すべきリストをくっつける
			String values = edgeset;
			Iterator<String> n = appendnodelist.iterator();
			while (n.hasNext()) {
				String appendline = returnline(Integer.parseInt(n.next()));
				String[] temp = appendline.split(":");
				String nodeline = temp[1];
				//System.out.println(nodeline);
				values = values + "\t" + nodeline;
			}
			System.out.println(values);
			context.write(new Text(nodenum), new Text(values));
		}catch (IOException e){
			System.out.println(e);
		}
	}
	
	// returnline: あるファイルの指定した行を返す
	public String returnline(int num){
		String line = "";
		try{
			FileReader in = new FileReader(FILENAME);
			BufferedReader br = new BufferedReader(in);
			//System.out.println("num: " + num + "in: " + in);
			for(int i = 0; i < num; i++){
				if((line = br.readLine()) == null){
					break;
				}
				//System.out.println("line" + line);
			}
			br.close();
		} catch (IOException e){
			System.out.println(e);
		}
		
		return line;
	}
}