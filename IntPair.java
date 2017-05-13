package mapreduce.advanced.demo.secondarysort;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class IntPair implements WritableComparable<IntPair>{

	private IntWritable first;
	private IntWritable second;
	
	public IntWritable getFirst() {
		return first;
	}
	
	public IntWritable getSecond() {
		return second;
	}

	public IntPair() {
		this.first = new IntWritable(0);
		this.second = new IntWritable(0);
	}
	
	public IntPair(int first, int second) {
		this.first = new IntWritable(first);
		this.second = new IntWritable(second);
	}
	
	public void set(int first, int second) {
		this.first.set(first);
		this.second.set(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	@Override
	public int compareTo(IntPair o) {
		// TODO Auto-generated method stub
		if (this.first.compareTo(o.first) == 0) {
			return (-1) * this.second.compareTo(o.second);
		}
		else {
			return this.first.compareTo(o.first);
		}
	}
	
	public String toString()
	{
		return first.toString() + "\t" + second.toString();
	}
	
	/*
	@Override
	public int hashCode(){
		return first.hashCode();
	}
	*/
}
