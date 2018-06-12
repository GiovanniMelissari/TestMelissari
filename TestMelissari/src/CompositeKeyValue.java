import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyValue implements WritableComparable<CompositeKeyValue> {
	
	private String solver;
	private double time;
	
	
	public CompositeKeyValue(String solver, double time) {
		super();
		this.solver = solver;
		this.time = time;
	}

	public CompositeKeyValue() {
		super();
		this.solver = new String();
		this.time = 0;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		solver = arg0.readUTF();
		time = arg0.readDouble();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(solver);
		arg0.writeDouble(time);
	}

	@Override
	public int compareTo(CompositeKeyValue o) {
		if(this.solver.toString().equals(o.getSolver().toString()))
			return Double.compare(this.time, o.getTime());
		
		return solver.toString().compareTo(o.getSolver().toString());
	}

	public String getSolver() {
		return solver;
	}

	public double getTime() {
		return time;
	}
	
	
	@Override
	public String toString() {
		return super.toString();
	}

}
