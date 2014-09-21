package ryosuke;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text, Text, Text, IntWritable> {
	static int START = 0, END = 1;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// mapから受け取った値をEdgeクラスに格納する
		Iterator<Text> iterator = values.iterator();
		List<Edge> edges = new ArrayList<Edge>();	
		while (iterator.hasNext())
		{
			String value = iterator.next().toString();
			//System.out.println(value);
			List<String> edge = Arrays.asList(value.split("\t"));
			//System.out.println(Arrays.asList(edge));
			Iterator<String> i = edge.iterator();
			while(i.hasNext()){
				String[] temp = i.next().split(",");
				edges.add(new Edge(temp[START], temp[END]));
			}
		}
		
		// クラスター係数の計算
		String origin = key.toString();
		
		// 隣接するノードを調べる
		List<String> adjnode = new ArrayList<String>();
		List<Edge> otheredges = new ArrayList<Edge>();
		Iterator<Edge> e = edges.iterator();
		while(e.hasNext()){
			Edge tempe = e.next();
			//System.out.printf("start: " + tempe.start + "end: " + tempe.end + "\n");
			//System.out.println(tempe.start);
			//System.out.println(origin);
			if(tempe.start.equals(origin)){
				adjnode.add(tempe.end);
			}else{
				otheredges.add(tempe);
				//System.out.printf("start: " + tempe.start + "end: " + tempe.end + "\n");
			}
		}
		//System.out.println(adjnode);
		//System.out.println(Arrays.asList(otheredges));
		/*
		Iterator<Edge> xx = otheredges.iterator();
		while(xx.hasNext()){
			Edge t = xx.next();
			System.out.println(t.start + t.end);
		}
		*/
		
		/*
		// 隣接するノード同士の組み合わせを見つける
		int trianglecount = 0;
		List<Edge> a = combi(adjnode);
		
		// クラスタ係数の三角形をカウントする
		Iterator<Edge> b = edges.iterator();
		while(b.hasNext()){
			Iterator<Edge> ite = a.iterator();
			while(ite.hasNext()){
				if(ite.next().start == b.next().start && ite.next().end == b.next().end){
					trianglecount++;
				}
			}
		}
		
		context.write(key, new IntWritable(trianglecount));
		*/
	}
	private List<Edge> combi(List<String> adjnode) {
		List<Edge> combi = null;
		Iterator<String> i = adjnode.iterator();
		while(i.hasNext()){
			Iterator<String> j = i;
			while(j.hasNext()){
				combi.add(new Edge(i.next(), j.next()));
			}
			i.remove();
		}
		return combi;
	}
	class Edge{
		String start, end;
		Edge(String start, String end){
			this.start = start;
			this.end = end;
		}
	}
}