import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

public class Data {

    private int numExamples;

//    private int[] id;
//    private int[] labels;
    private TreeMap<Integer, int[]> data;     // map (feature id) -> (list of example id)
    private TreeMap<Integer, Integer> labels; // map (example id) -> (0/1 label)

    public Data(TreeMap<Integer,Integer> labels, TreeMap<Integer,int[]> data){
        this.data = data;
        this.labels = labels;
        this.numExamples = labels.size();
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

    // TODO information gain
    public double computeScore(int feat){
        int cnt = 0;
        int[] examples = data.get(new Integer(feat));
        if (examples != null){
            for (int i=0; i < examples.length; i++){
                cnt += (labels.get(examples[i]).intValue() == 1) ? 1 : 0;
            }
//            if (cnt != 0)
//                System.out.println("cnt = " + cnt);
        }
        return Math.abs(numExamples/2 - cnt);
    }

    public DataPair split(int feat){
        HashSet<Integer> index = new HashSet<Integer>();
        int[] examples = data.get(new Integer(feat));
        if (examples == null)
            return null;

        for (int i=0; i < examples.length; i++){
            index.add( examples[i] );
        }

        /*
        int[] leftLabels = new int[index.size()];
        int[] rightLabels = new int[numExamples - index.size()];
        int[] leftID = new int[index.size()];
        int[] rightID = new int[numExamples - index.size()];
        int nLeft = 0, nRight = 0;
        for (int i=0; i < numExamples; i++){
            if (index.contains(id[i])){
                leftID[nLeft] = id[i];
                leftLabels[nLeft++] = labels[i];
            }else{
                rightID[nRight] = id[i];
                rightLabels[nRight++] = labels[i];
            }
        }
        assert nLeft == index.size();
        assert nRight == numExamples - index.size();
        */
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
//        Data leftData = new Data(leftID, leftMap, leftLabels);
//        Data rightData = new Data(rightID, rightMap, rightLabels);
        Data leftData = new Data(leftLabels, leftMap);
        Data rightData = new Data(rightLabels, rightMap);
        System.out.println("split = " + leftData.getNumExamples() + " " + rightData.getNumExamples());
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
