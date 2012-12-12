package edu.umd.rf.RandomForest;

import java.util.ArrayList;

public class RandomForest{

    protected int numTrees;
    protected int maxDepth;
    protected Tree[] trees;

    public RandomForest(int numTrees, int maxDepth){
        this.numTrees = numTrees;
        this.maxDepth = maxDepth;
        this.trees = new Tree[numTrees];
    }

    public void train(Data data){
        for (int i = 0; i < this.numTrees; i++){
            System.err.printf("training %dth tree\n", i);
            long startTime = System.currentTimeMillis();
            
            this.trees[i] = new Tree(maxDepth);
            this.trees[i].train(data);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.println("--- Elapsed Time of tree " + i+ " = " + elapsedTime / 1000.0);     

        }
    }

    public int predict(ArrayList<Integer> data){
        int c0=0, c1=0;
        for (int i=0; i<numTrees; i++){
            int res = this.trees[i].predict(data);
            if (res == 0)
                c0++;
            else if (res == 1)
                c1++;
            assert res==0 || res==1;
        }
        return (c0 > c1) ? 0 : 1;
    }
    
    public int getMaxDepth(){
    	return maxDepth;
    }

    public double size(){
    	double x = 0;
    	for (int i=0; i<trees.length; i++) x += trees[i].size();
    	return x / trees.length;
    }

}
