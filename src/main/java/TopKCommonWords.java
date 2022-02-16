/**
 *  Matric Number: A0182488N
 *  Name: Suther David Samuel
 *  CS4225 Programming Assignment 1
 */

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class TopkCommonWords {

    private static final int K_VALUE = 20;

    private static final boolean ON_CLUSTER = false;

    private static String inputFileOneName;
    public static class CountWord implements WritableComparable<CountWord> {

        private IntWritable count;
        private Text word;

        public CountWord() {
            this.count = new IntWritable();
            this.word = new Text();
        }

        public CountWord(int count, String word) {
            this.count = new IntWritable(count);
            this.word = new Text(word);
        }

        public IntWritable getCount() {
            return count;
        }

        public void setCount(IntWritable count) {
            this.count = count;
        }

        public Text getWord() {
            return word;
        }

        public void setWord(Text word) {
            this.word = word;
        }

        @Override
        public int compareTo(CountWord o) {
            if (this.count.get() < o.count.get()) {
                return 1;
            } else if (this.count.get() > o.count.get()) {
                return -1;
            } else {
                // descending lexicographic comparison
                return -1 * this.word.compareTo(o.getWord());
            }
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.word.write(dataOutput);
            this.count.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.word.readFields(dataInput);
            this.count.readFields(dataInput);
        }

        @Override
        public String toString() {
            return count.toString() + "\t" + word.toString();
        }

        public CountWord clone() {
            CountWord clone = new CountWord();
            clone.setCount(new IntWritable(this.count.get()));
            clone.setWord(new Text(this.word));
            return clone;
        }
    }


    public static class CommonWordsMapper extends Mapper<Object, Text, CountWord, IntWritable> {
        private Set<String> stopWords;
        private Map<String, Integer> wordToDocumentMap;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            wordToDocumentMap = new HashMap<>();
            stopWords = new HashSet<>();
            // get all the stop words over here
            URI[] localpaths = context.getCacheFiles();
            inputFileOneName = context.getConfiguration().get("input_one_file");
            try {
                // @TODO update the file link -> get from the terminal input
                File stopwordsFile = new File(localpaths[0].getPath());
                System.out.println("File path: " + stopwordsFile.getAbsolutePath());
                BufferedReader bufferedReader;
                if (ON_CLUSTER) {
                    bufferedReader = new BufferedReader(new FileReader(stopwordsFile.getName()));
                } else {
                    bufferedReader = new BufferedReader(new FileReader(stopwordsFile));
                }
                String stopword;
                while ((stopword = bufferedReader.readLine()) != null) {
                    stopWords.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception occurred during parsing stopwords file: " + StringUtils.stringifyException(e));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            String currWord;
            while (itr.hasMoreTokens()) {
                currWord = itr.nextToken();
                if (!stopWords.contains(currWord)) {
                    if (wordToDocumentMap.get(currWord) == null) {
                        wordToDocumentMap.put(currWord, 1);
                    } else {
                        wordToDocumentMap.put(currWord, wordToDocumentMap.get(currWord) + 1);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, CountWord, IntWritable>.Context context) throws IOException,
                InterruptedException {
            Integer documentId = (((FileSplit)context.getInputSplit()).getPath().getName().equals(inputFileOneName)) ? 0 : 1;
            for (String key: wordToDocumentMap.keySet()) {
                context.write(new CountWord(wordToDocumentMap.get(key), key), new IntWritable(documentId));
            }
        }
    }

//    public static class CommonWordsCombiner extends Reducer<CountWord, IntWritable, CountWord, IntWritable> {
//
//        private Map<String, ArrayList<CountWord>> combinerHashMap;
//
//        @Override
//        protected void setup(Reducer<CountWord, IntWritable, CountWord, IntWritable>.Context context) throws IOException, InterruptedException {
//            super.setup(context);
//            combinerHashMap = new HashMap<>();
//        }
//
//        @Override
//        protected void reduce(CountWord key, Iterable<IntWritable> values, Reducer<CountWord, IntWritable, CountWord,
//                IntWritable>.Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
//            for (IntWritable val: values) {
//                combinerHashMap.computeIfAbsent(key.getWord().toString(), k -> new ArrayList<>(2));
//                CountWord currCountWord = combinerHashMap.get(key.getWord().toString()).get(val.get());
//                if (currCountWord == null) {
//                    combinerHashMap.get(key.getWord().toString()).set(val.get(), key);
//                } else {
//                    currCountWord.setCount(new IntWritable(currCountWord.getCount().get() + key.getCount().get()));
//                    combinerHashMap.get(key.getWord().toString()).set(val.get(), currCountWord);
//                }
//            }
//        }
//
//        @Override
//        protected void cleanup(Reducer<CountWord, IntWritable, CountWord, IntWritable>.Context context) throws IOException, InterruptedException {
//            super.cleanup(context);
//            for (String key: combinerHashMap.keySet()) {
//                for (int i = 0; i < combinerHashMap.get(key).size(); i++) {
//                    if (combinerHashMap.get(key).get(i) != null) {
//                        context.write(combinerHashMap.get(key).get(i), new IntWritable(i));
//                    }
//                }
//            }
//        }
//    }

    public static class CommonWordsReducer extends Reducer<CountWord, IntWritable, IntWritable, Text> {

        private TreeSet<CountWord> treeSet;
        private Map<String, ArrayList<CountWord>> reducerHashMap;

        @Override
        protected void setup(Reducer<CountWord, IntWritable, IntWritable, Text>.Context context) throws IOException,
                InterruptedException {
            reducerHashMap = new HashMap<>();
            treeSet = new TreeSet();
        }

        @Override
        protected void reduce(CountWord key, Iterable<IntWritable> values, Reducer<CountWord, IntWritable,
                IntWritable, Text>.Context context) throws IOException, InterruptedException {
            for (IntWritable val: values) {
                reducerHashMap.computeIfAbsent(key.getWord().toString(), k -> new ArrayList<>(Arrays.asList(null, null)));
                CountWord currCountWord = reducerHashMap.get(key.getWord().toString()).get(val.get());
                if (currCountWord == null) {
                    reducerHashMap.get(key.getWord().toString()).set(val.get(), key.clone());
                } else {
                    currCountWord.setCount(new IntWritable(currCountWord.getCount().get() + key.clone().getCount().get()));
                    reducerHashMap.get(key.getWord().toString()).set(val.get(), currCountWord);
                }
            }
        }

        @Override
        protected void cleanup(Reducer<CountWord, IntWritable, IntWritable, Text>.Context context) throws IOException
                , InterruptedException {
            int countOne;
            int countTwo;
            // iterate through hashmap to find the max of each word
            for(String word: reducerHashMap.keySet()) {
                if (reducerHashMap.get(word).get(0) == null) {
                    continue;
                }
                if (reducerHashMap.get(word).get(1) == null) {
                    continue;
                }
                countOne = reducerHashMap.get(word).get(0).getCount().get();
                countTwo = reducerHashMap.get(word).get(1).getCount().get();
                if (countOne > 0 && countTwo > 0) {
                    System.out.println("this is the word: " + word);
                    treeSet.add(new CountWord(Math.max(countOne, countTwo), word));
                    if (treeSet.size() > K_VALUE) {
                        treeSet.remove(treeSet.last());
                    }
                }
            }

            // treeset now contains top 20
            for (CountWord countWord: treeSet) {
                context.write(countWord.getCount(), countWord.getWord());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("input_one_file", new Path(args[0]).getName());
        Job job = Job.getInstance(conf, "TopKCommonWords");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(CommonWordsMapper.class);
        // job.setCombinerClass(CommonWordsCombiner.class);
        job.setReducerClass(CommonWordsReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(CountWord.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        if (ON_CLUSTER) {
            job.addCacheFile(new URI(args[2]));
        } else {
            job.addCacheFile(new Path(args[2]).toUri());
        }

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        boolean success = job.waitForCompletion(true);
        if (!success) {
            System.out.println("Error occurred");
            System.exit(1);
        }
        System.out.println("Successfully completed job");
        System.exit(0);
    }
}
