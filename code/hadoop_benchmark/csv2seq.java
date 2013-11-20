import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

public class csv2seq {
    // args[0]: input file; args[1]: nrows; args[2]: output file
    public static void main(String args[])  throws IOException{
	final int nrows = Integer.parseInt(args[1]);
        final Configuration conf = new Configuration();
	final FileSystem fs = FileSystem.get(conf);
	final SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(args[2]), 
		IntWritable.class, VectorWritable.class, CompressionType.BLOCK);
	final IntWritable key = new IntWritable();
	final VectorWritable value = new VectorWritable();

	String line;
	BufferedReader br = new BufferedReader(new FileReader(args[0]));
	Vector vector = null;
	int last_col = -1;	
	while ((line = br.readLine()) != null) {
	    String[] parts = line.split(",");
            // get data in line
	    int row = Integer.parseInt(parts[0].trim());
	    int col = Integer.parseInt(parts[1].trim());
	    float val = Float.parseFloat(parts[2].trim()); 
	
	    if (last_col != col) {
		if (last_col != -1) {
		    value.set(vector);
		    writer.append(key, value);
		}
	    	last_col = col;
                vector = new SequentialAccessSparseVector(nrows);
		key.set(col);
	    }
	    vector.set(row, val);
	}
	value.set(vector);
        writer.append(key,value);
	writer.close();
    }
}
