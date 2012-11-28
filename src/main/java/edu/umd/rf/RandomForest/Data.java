package edu.umd.rf.RandomForest;

import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

public class Data {

    private int numExamples;
    private int numPositive;

    private TreeMap<Integer, int[]> data;     // map (feature id) -> (list of example id)
    private TreeMap<Integer, Integer> labels; // map (example id) -> (0/1 label)

    public Data(TreeMap<Integer,Integer> labels, TreeMap<Integer,int[]> data){
        this.data = data;
        this.labels = labels;
        this.numExamples = labels.size();
        for (Integer label : labels.values())
            numPositive += (label.intValue() == 1) ? 1 : 0;
    }

    public int getMajority(){
        int[] c = {0, 0};
        for (Integer label : labels.values()){
            c[label.intValue()]++;
        }
        return (c[0] > c[1]) ? 0 : 1;
    }

    public int[] featureRandomSubset(int size){
        Random rand = new Random();
        Integer[] features = data.keySet().toArray(new Integer[0]);
        int[] a = new int[size];
        for (int i=0; i<size; i++){
            a[i] = features[rand.nextInt(features.length)].intValue();
        }
        return a;
    }

    private double entropy(double x,double y){
        double p = x / y;
        return -(p * Math.log(p) + (1-p) * Math.log(1-p));
    }

    private double gini(double x,double y){
        double p = x / y;
        return p * (1-p);
    }


    // TODO information gain
    // Gini index used now
    public double computeScore(int feat){
        int[] examples = data.get(new Integer(feat));
        if (examples == null)
            return -1e30;

        int cnt = 0;
        for (int i=0; i<examples.length; i++){
            cnt += (labels.get(examples[i]).intValue() == 1) ? 1 : 0;
        }
        if (numExamples == examples.length)
            return -1e30;
        double g = ((double)examples.length / numExamples * gini(cnt, examples.length) + 
                (double)(numExamples - examples.length) / numExamples * gini(numPositive - cnt, numExamples - examples.length));
        /*
        if (g!=g){
            System.out.printf("%f %f %f %f %f %d/%d\n", g , (double)examples.length / numExamples , gini(cnt, examples.length) , (double)(numExamples - examples.length) , numExamples * gini(numPositive - cnt, numExamples - examples.length), numExamples, examples.length);
        }
        */
        return g;
    }

    public DataPair split(int feat){
        HashSet<Integer> index = new HashSet<Integer>();
        int[] examples = data.get(new Integer(feat));
        if (examples == null)
            return null;

        for (int i=0; i < examples.length; i++){
            index.add( examples[i] );
        }

        TreeMap<Integer, Integer> leftLabels = new TreeMap<Integer,Integer>();
        TreeMap<Integer, Integer> rightLabels = new TreeMap<Integer,Integer>();
        for (Map.Entry<Integer,Integer> entry : labels.entrySet()){
            if (index.contains( entry.getKey() )){
                leftLabels.put( entry.getKey(), entry.getValue());
            }else{
                rightLabels.put( entry.getKey(), entry.getValue());
            }
        }

        TreeMap<Integer, int[]> leftMap  = new TreeMap<Integer, int[]>();
        TreeMap<Integer, int[]> rightMap = new TreeMap<Integer, int[]>();
        for (Map.Entry<Integer, int[]> entry : data.entrySet()){
            ArrayList<Integer> leftList = new ArrayList<Integer>();
            ArrayList<Integer> rightList = new ArrayList<Integer>();

            examples = entry.getValue();
            for (int j=0; j < examples.length; j++){
                if (index.contains(examples[j])){
                    leftList.add(examples[j]);
                }else{
                    rightList.add(examples[j]);
                }
            }
            if (leftList.size() > 0)
                leftMap.put(entry.getKey(), toIntArray(leftList));
            if (rightList.size() > 0)
                rightMap.put(entry.getKey(), toIntArray(rightList));
        }
        Data leftData = new Data(leftLabels, leftMap);
        Data rightData = new Data(rightLabels, rightMap);
//        System.out.printf("split = %d(%d) | %d(%d)\n" , leftData.getNumExamples(), leftData.getMajority(), rightData.getNumExamples(), rightData.getMajority());
        return new DataPair(leftData, rightData);
    }

    public static int[] toIntArray(ArrayList<Integer> list){
        int[] a = new int[list.size()];
        for (int i=0; i<a.length; i++){
            a[i] = list.get(i).intValue();
        }
        return a;
    }

    public boolean monotone(){
        int x = 0;
        for (Integer label : labels.values())
            x += label.intValue();
        if (x>=0)
        return x==0 || x==labels.size();

        Integer firstLabel = null;
        for (Integer label : labels.values()){
            if (firstLabel == null) 
                firstLabel = label;
            if (!label.equals(firstLabel))
                return false;
        }
        return true;
    }

    public int getNumExamples(){
        return this.numExamples;
    }
}
