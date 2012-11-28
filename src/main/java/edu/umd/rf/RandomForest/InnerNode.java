package edu.umd.rf.RandomForest;

import java.util.ArrayList;

public class InnerNode extends Node{

    private int feat;
    private Node left;
    private Node right;

    public InnerNode(int feat, Node left, Node right){
        this.feat = feat;
        this.left = left;
        this.right = right;
    }
    
    public int predict(ArrayList<Integer> a){
        return a.contains(new Integer(feat)) ? left.predict(a) : right.predict(a);
    }
}
