import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimeComparator extends WritableComparator {
	
	public TimeComparator() {
		super(CompositeKeyValue.class, true);
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyValue aa = (CompositeKeyValue) a;
		CompositeKeyValue bb = (CompositeKeyValue) b;
		
		return aa.compareTo(bb);
	}
}
