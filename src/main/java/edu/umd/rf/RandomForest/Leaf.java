package edu.umd.rf.RandomForest;

import java.util.ArrayList;

public class Leaf extends Node{

    private int majority;

    public Leaf(int majority){
        /*
        if (majority == 1){
            System.out.println("positive leaf node!!!!");
        }else{
            System.out.println("negative leaf node!!!!");
        }
        */
        this.majority = majority;
    }

    public int predict(ArrayList<Integer> a){
        return majority;
    }
}
