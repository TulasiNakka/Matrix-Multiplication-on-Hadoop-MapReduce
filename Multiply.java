import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Element implements Writable {
	int tag;
	int index;
	double value;
	
	Element() {
		tag = 0;
		index = 0;
		value = 0.0;
	}
	
	Element(int tag, int index, double value) {
		this.tag = tag;
		this.index = index;
		this.value = value;
	}
	
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(tag);
		output.writeInt(index);
		output.writeDouble(value);
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		tag = input.readInt();
		index = input.readInt();
		value = input.readDouble();
	}

public Element copy() {
        return new Element(tag, index, value);
    }
}

class Pair implements WritableComparable<Pair> {

	int i;
	int j;
	
	Pair() {
		i = 0;
		j = 0;
	}
	
	Pair(int i, int j) {
		this.i = i;
		this.j = j;
	}
	
	
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(i);
		output.writeInt(j);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		i = input.readInt();
		j = input.readInt();
	}

	
	@Override
	public int compareTo(Pair c1) {
		int c2 = Integer.compare(i, c1.i);
		if ( c2!= 0) {
			return c2;
		}
		
		return Integer.compare(j, c1.j);
	}
    
	@Override
	public String toString() {
		return i + " " + j + " ";
		//return i +"," +j +" ";
	}
	
}

public class Multiply {

	//First Map-Reduce job:

	public static class Mapper_for_M extends Mapper<Object,Text,IntWritable,Element> {
		//Mapper M
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
    
			
			String[] array_index = value.toString().split(",");
			int index = Integer.parseInt(array_index[0]);
			double double_value = Double.parseDouble(array_index[2]);
			Element e = new Element(0,index, double_value);
			IntWritable keyValue = new IntWritable(Integer.parseInt(array_index[1]));
			context.write(keyValue, e);
			
		}
	}

	public static class Mapper_for_N extends Mapper<Object,Text,IntWritable,Element> {
		// Mapper N
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] array_index = value.toString().split(",");
			int index = Integer.parseInt(array_index[1]);
			double double_value = Double.parseDouble(array_index[2]);

			Element e = new Element(1,index, double_value);
			
			IntWritable keyValue = new IntWritable(Integer.parseInt(array_index[0]));
			context.write(keyValue, e);
		}
	}

	public static class Reducer1 extends Reducer<IntWritable,Element, Pair, DoubleWritable> {
		
	// Calculating product 
	@Override
	public void reduce(IntWritable key, Iterable<Element> values, Context context) 
			throws IOException, InterruptedException {
		
		ArrayList<Element> A = new ArrayList<Element>();
		ArrayList<Element> B = new ArrayList<Element>();
		Configuration conf = context.getConfiguration();
		A.clear();
		B.clear();
		for(Element element : values) {
             
			Element element_copy = element.copy();
            
			if (element_copy.tag == 0) {
				A.add(element_copy);
			} else if(element_copy.tag == 1) {
				B.add(element_copy);
			}
		}

		for(Element a : A){
			for (Element b : B){
				
				context.write(new Pair(a.index,b.index),new DoubleWritable(a.value*b.value));
			}
		}
		
		}
	}
	
	// Second Map-Reduce job:
	
	public static class Mapper2 extends Mapper<Object, Text, Pair, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			//do nothing just passing the values same like Mapper1
			
			String readLine = value.toString();
			String[] index_p = readLine.split(" ");	
			Pair pair = new Pair(Integer.parseInt(index_p[0]),Integer.parseInt(index_p[1]));
			DoubleWritable map2_val = new DoubleWritable(Double.parseDouble(index_p[2]));
			context.write(pair, map2_val);   
		}
	}
	
	public static class Reducer2 extends Reducer<Pair, DoubleWritable,Text,Text> {

		// Calculating aggregation
		@Override
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException {
			
			Double m = 0.0;
			for( DoubleWritable v : values) {
				m += v.get();
				
			}
			Text outputKey = new Text(key.i + "," + key.j);
			Text outputValue = new Text(m.toString());

			context.write(outputKey, outputValue);
		}
	}


/*
@Override
    public int run ( String[] args ) throws Exception {
        
        return 0;
    }
*/
	public static void main(String[] args) throws Exception {
		
		// Job 1
		Job job = Job.getInstance();
		job.setJobName("Job 1");
		job.setJarByClass(Multiply.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper_for_M.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper_for_N.class);
		job.setReducerClass(Reducer1.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Element.class);
		
		job.setOutputKeyClass(Pair.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
		
		
		//Job 2 
		Job job2 = Job.getInstance();
		job2.setJobName("Job 2");
		job2.setJarByClass(Multiply.class);
		
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		
		job2.setMapOutputKeyClass(Pair.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		
		job2.setOutputKeyClass(Pair.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		
		job2.waitForCompletion(true);
	}
}

   


