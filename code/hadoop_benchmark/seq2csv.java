import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Iterator;

import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class seq2csv {
	public static int Cardinality;

	public static void main(String[] args){
		try {
			final Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get(conf);
			final SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(args[0]), conf);
			BufferedWriter br = new BufferedWriter(new FileWriter(args[1]));
			IntWritable key = new IntWritable();
			VectorWritable vec = new VectorWritable();

			while (reader.next(key, vec)) {
				SequentialAccessSparseVector vect = (SequentialAccessSparseVector)vec.get();
				Iterator<Vector.Element> iter = vect.iterateNonZero();
				while(iter.hasNext()){
					Vector.Element element = iter.next();
					br.write(key + "," + element.index() + "," + vect.getQuick(element.index())+"\n");
				}
			}
			reader.close();
			br.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
