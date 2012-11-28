package edu.umd.rf.RandomForest;


public class DataPair{

    private Data left;
    private Data right;

    public DataPair(Data left, Data right){
        this.left = left;
        this.right = right;
    }

    public Data left(){
        return left;
    }

    public Data right(){
        return right;
    }
}
