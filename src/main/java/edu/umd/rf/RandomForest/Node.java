package edu.umd.rf.RandomForest;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public abstract class Node{
    public abstract int predict(ArrayList<Integer> a);
    
    public void writeTree(DataOutput out) throws IOException{
   		out.writeBoolean(this instanceof Leaf);
    }

	public abstract int size();
}
