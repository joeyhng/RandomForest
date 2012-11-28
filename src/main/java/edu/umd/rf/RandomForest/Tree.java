package edu.umd.rf.RandomForest;

import java.util.ArrayList;

public class Tree{

    private static int subsetSize = 30000;
    private int maxDepth;
    private Node root;

    public Tree(int maxDepth){
        this.maxDepth = maxDepth;
    }

    public void train(Data data){
        this.root = train_helper(data, 0);
    }

    private Node train_helper(Data data, int depth){

        if (depth == this.maxDepth || data.monotone()){
            return new Leaf(data.getMajority());
        }

        int[] selectedFeatures = data.featureRandomSubset(subsetSize);
        double maxScore = -1e30; 
        int bestFeat = -1;
        for (int f = 0; f < selectedFeatures.length; f++){
            int feat = selectedFeatures[f];
            double score = data.computeScore(feat);
            if (bestFeat == -1 || maxScore > score){
                bestFeat = feat;
                maxScore = score;
            }
        }
//        System.out.println("max score = " + maxScore);

        if (maxScore <= 0){
            return new Leaf(data.getMajority());
        }

        DataPair dataSplit = data.split(bestFeat);
        if (dataSplit == null){
            return new Leaf(data.getMajority());
        }
        data = null;
        Node leftNode = train_helper(dataSplit.left(), depth+1);
        Node rightNode = train_helper(dataSplit.right(), depth+1);
        Node res = new InnerNode(bestFeat, leftNode, rightNode);
        if (leftNode.getClass().getName().equals("InnerNode"))
            System.out.println(leftNode.getClass().getName() + " " + rightNode.getClass().getName());
        return res;
    }

    public int predict(ArrayList<Integer> data){
        return root.predict(data);
    }

}
