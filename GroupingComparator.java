package mapreduce.advanced.demo.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{
	
	protected GroupingComparator() {
		super(IntPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		IntPair ip1 = (IntPair) w1;
		IntPair ip2 = (IntPair) w2;
		
		if (ip1.getFirst().get() != ip2.getFirst().get()) {
			return (ip1.getFirst().get() > ip2.getFirst().get()) ? 1 : -1;
		}
		else {
			return 0;
		}
	}
}
