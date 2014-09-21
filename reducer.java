package ryosuke;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
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
		LinkedList<String> adjnode = new LinkedList<String>();
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
		System.out.println(adjnode);
		//System.out.println(Arrays.asList(otheredges));
		
		
		// 隣接するノード同士の組み合わせを見つける
		int trianglecount = 0;
		List<Edge> combination = combi(adjnode);
		
		/*
		Iterator<Edge> yy = edges.iterator();
		while(yy.hasNext()){
			Edge t = yy.next();
			System.out.println(t.start + t.end);
		}
		System.out.println("=========================");
		
		Iterator<Edge> xx = combination.iterator();
		while(xx.hasNext()){
			Edge t = xx.next();
			System.out.println(t.start + t.end);
		}
		
		System.out.println("=========================");
		*/
		// クラスタ係数の三角形をカウントする
		for(Edge ed : edges){
			for(Edge com : combination){
				if(ed.start.equals(com.start) && ed.end.equals(com.end)){
					trianglecount++;
				}
			}
		}
		/*
		Iterator<Edge> b = edges.iterator();
		while(b.hasNext()){
			Iterator<Edge> ite = a.iterator();
			while(ite.hasNext()){
				if(ite.next().start == b.next().start && ite.next().end == b.next().end){
					trianglecount++;
				}
			}
		}*/
		System.out.println(key+": "+trianglecount);
		/*
		context.write(key, new IntWritable(trianglecount));
		*/
	}
	private List<Edge> combi(LinkedList<String> adjnode) {
		List<Edge> combi = new LinkedList<reducer.Edge>();
		//Iterator<String> i = adjnode.iterator();
		//System.out.println("=======start========");
		//System.out.println(Arrays.asList(adjnode));
		while(!adjnode.isEmpty()){
			//System.out.printf("*");
			String s = adjnode.pop();
			for(String adj : adjnode){
				combi.add(new Edge(s, adj));
			}
		}
		//System.out.printf("\n");
		/*
		while(i.hasNext()){
			String one = i.next();
			Iterator<String> j = i;
			while(j.hasNext()){
				//combi.add(new Edge(i.next(), j.next()));
				System.out.println(one + j.next());
			}
			//i.remove();
		}
		*/
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