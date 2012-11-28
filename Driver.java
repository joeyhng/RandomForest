import java.util.ArrayList;
import java.util.TreeMap;
import java.io.File;
import java.util.Scanner;
import java.io.IOException;

public class Driver{

    public Data readData(String filename) throws IOException{
        
        int numFeatures = 685570;
        TreeMap<Integer,Integer> labels = new TreeMap<Integer,Integer>();
//        ArrayList<Integer> idList = new ArrayList<Integer>();
//        ArrayList<Integer> labelsList = new ArrayList<Integer>();
        ArrayList[] dataList = new ArrayList[numFeatures];
        for (int i=0; i<numFeatures; i++){
            dataList[i] = new ArrayList<Integer>();
        }

        File file = new File(filename);
        Scanner fileScanner = new Scanner(file);
        while (fileScanner.hasNextLine()){
            String line = fileScanner.nextLine().replaceAll(":", " ");
            Scanner sc = new Scanner(line);
 //           int idx = idList.size();
 //           labelsList.add(sc.nextInt());
 //           idList.add(idx);
            int idx = labels.size();
            int lab = sc.nextInt();
            labels.put( idx, lab );

            while (sc.hasNextInt()){
                int attr = sc.nextInt();
                dataList[attr].add( idx );
                sc.next();
            }
        }

//        int[] id = Data.toIntArray(idList);
//        int[] labels = Data.toIntArray(labelsList);
        TreeMap<Integer, int[]> data = new TreeMap<Integer,int[]>();
        for (int i=0; i < numFeatures; i++){
            if (dataList[i].size() > 0)
                data.put(new Integer(i), Data.toIntArray(dataList[i]));
        }
//        return new Data(id, data, labels);
        return new Data(labels, data);
    }

    public void testRF(String filename, RandomForest rf) throws IOException{
        int tp = 0, fp = 0, tn = 0, fn = 0;
        int yes = 0, total = 0;
        File file = new File(filename);
        Scanner fileScanner = new Scanner(file);
        while (fileScanner.hasNextLine()){
            String line = fileScanner.nextLine();
            Scanner sc = new Scanner(line);

            int label = sc.nextInt();
            ArrayList<Integer> data = new ArrayList<Integer>();
            while (sc.hasNextInt()){
                int attr = sc.nextInt();
                data.add( attr );
                sc.next();
            }

            int pred = rf.predict(data);
            if (pred == 1){
                if (label == 1) tp++;
                else fp++;
            }else{
                if (label == 0) tn++;
                else fn++;
            }
            if (label == pred) yes++;
            total++;
        }
        System.out.printf("accuracy = %d / %d = %f\n", yes, total, (double)yes / total);
        System.out.printf("precision = %d / %d = %f\n", tp, tp+fp, (double)tp / (tp + fp));
        System.out.printf("recall = %d / %d = %f\n", tp, tp+fn, (double)tp / (tp + fn));
        System.out.printf("tp=%d, fp=%d, tn=%d, fn=%d\n", fp, fp, tn, fn);
    }

    public void run() throws IOException{
        System.err.println("start reading data");
        Data data = readData("input/train_Blanc__Mel.txt");
        System.err.println("finish reading data, start training random forest");
        RandomForest rf = new RandomForest(10, 20);
        rf.train(data);
        System.err.println("finish training data, now start testing");
        testRF("input/test_Blanc__Mel.txt", rf);
    }

    public static void main(String args[]) throws IOException{
        (new Driver()).run();
    }
}
