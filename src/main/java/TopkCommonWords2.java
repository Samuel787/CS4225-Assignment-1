import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class TopkCommonWords2 {

    private static final int K_VALUE = 20;

    public static class CountWord implements WritableComparable<CountWord> {

        public IntWritable getmCount() {
            return mCount;
        }

        public void setmCount(IntWritable mCount) {
            this.mCount = mCount;
        }

        public Text getmWord() {
            return mWord;
        }

        public void setmWord(Text mWord) {
            this.mWord = mWord;
        }

        private IntWritable mCount;
        private Text mWord;

        public CountWord() {
            this.mCount = new IntWritable();
            this.mWord = new Text();
        }

        @Override
        public int compareTo(CountWord o) {
            if (this.mCount.get() < o.mCount.get()) {
                return 1;
            } else if (this.mCount.get() > o.mCount.get()) {
                return -1;
            } else {
                return -1 * this.mWord.compareTo(o.mWord);
            }
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.mWord.write(dataOutput);
            this.mCount.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.mWord.readFields(dataInput);
            this.mCount.readFields(dataInput);
        }

        @Override
        public String toString() {
            return mCount.toString() + "\t" + mWord.toString();
        }

        public CountWord clone() {
            CountWord clone = new CountWord();
            clone.setmCount(new IntWritable(this.mCount.get()));
            clone.setmWord(new Text(this.mWord));
            return clone;
        }
    }



    public static class WordToDocumentMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable isInput1 = new IntWritable(0);
        private final static IntWritable notInput1 = new IntWritable(1);
        private Set<String> stopWords = new HashSet<String>();

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            // get all the stop words over here
            URI[] localpaths = context.getCacheFiles();
            try {
                // @TODO update the file link -> get from the terminal input
                File stopwordsFile = new File(localpaths[0].getPath());
                System.out.println("File path: " + stopwordsFile.getAbsolutePath());
                BufferedReader bufferedReader = new BufferedReader(new FileReader(stopwordsFile));
                String stopword;
                while ((stopword = bufferedReader.readLine()) != null) {
                    stopWords.add(stopword);
                }
            } catch (IOException e) {
                System.err.println("Exception occurred during parsing stopwords file: " + StringUtils.stringifyException(e));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String documentName = ((FileSplit)context.getInputSplit()).getPath().getName();
            System.out.println("Document name: " + documentName);
            IntWritable val = (documentName.equals("task1-input1.txt")) ? isInput1 : notInput1;
            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f");
            String currWord;
            while (itr.hasMoreTokens()) {
                currWord = itr.nextToken();
                if (!stopWords.contains(currWord)) {
                    word.set(currWord);
                    context.write(word, val);
                }
            }
        }
    }

    public static class WordToDocumentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable maxOfCount = new IntWritable();
            int firstDocCount = 0;
            int secondDocCount = 0;
            boolean inDocOne = false;
            boolean inDocTwo = false;
            for (IntWritable val: values) {
                if (val.get() == 0) {
                    inDocOne = true;
                    firstDocCount += 1;
                } else {
                    inDocTwo = true;
                    secondDocCount += 1;
                }
            }
            if (inDocOne && inDocTwo) {
                int maxDocCount = Math.max(firstDocCount, secondDocCount);
                maxOfCount.set(maxDocCount);
                context.write(key, maxOfCount);
            }
        }
    }

    public static class CountToWordMapper extends Mapper<Object, Text, CountWord, IntWritable> {

        private TreeSet<CountWord> treeSet;

        @Override
        protected void setup(Mapper<Object, Text, CountWord, IntWritable>.Context context) throws IOException,
                InterruptedException {
            treeSet = new TreeSet();
            super.setup(context);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            CountWord countWord = new CountWord();
            StringTokenizer itr = new StringTokenizer(value.toString());
            String currWord;
            int currCount;
            while (itr.hasMoreTokens()) {
                currWord = itr.nextToken();
                // @TODO try catch in notnumberexception
                currCount = Integer.parseInt(itr.nextToken());
                countWord.setmCount(new IntWritable(currCount));
                countWord.setmWord(new Text(currWord));
                treeSet.add(countWord.clone());
                if (treeSet.size() > K_VALUE) {
                    treeSet.remove(treeSet.last());
                }
            }
        }

        @Override
        protected void cleanup(Mapper<Object, Text, CountWord, IntWritable>.Context context) throws IOException,
                InterruptedException {
            for (CountWord countWord: treeSet) {
                context.write(countWord, countWord.getmCount());
            }
            super.cleanup(context);
        }
    }

    public static class CountToWordPartitioner extends Partitioner<CountWord, IntWritable> {
        @Override
        public int getPartition(CountWord countWord, IntWritable intWritable, int numPartitions) {
            return Math.abs(countWord.mCount.hashCode()) % numPartitions;
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(CountWord.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CountWord cw1 = (CountWord) a;
            CountWord cw2 = (CountWord) b;
            return cw1.compareTo(cw2);
        }
    }

    public static class CountToWordReducer extends Reducer<CountWord, IntWritable, IntWritable, Text> {

        private TreeSet<CountWord> treeSet;

        @Override
        protected void setup(Reducer<CountWord, IntWritable, IntWritable, Text>.Context context) throws IOException,
                InterruptedException {
            System.out.println("I'm inside reducer set up");
            treeSet = new TreeSet();
            super.setup(context);
        }

        public void reduce(CountWord countWord, Iterable<IntWritable> h, Context context) throws IOException, InterruptedException {
            treeSet.add(countWord.clone());
            if (treeSet.size() > K_VALUE) {
                treeSet.remove(treeSet.last());
            }
        }

        @Override
        protected void cleanup(Reducer<CountWord, IntWritable, IntWritable, Text>.Context context) throws IOException
                , InterruptedException {
            for (CountWord countWord: treeSet) {
                context.write(countWord.getmCount(), countWord.getmWord());
            }
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {

        // temp directory for intermediate results
        String outputTempDir = System.getProperty("user.dir") + File.separator + "temp";
        File tempFile = new File(outputTempDir);
        FileUtils.deleteDirectory(tempFile);

        // directory for output
        FileUtils.deleteDirectory(new File(args[3]));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopKCommonWords");
        job.setJarByClass(TopkCommonWords2.class);
        job.setMapperClass(WordToDocumentMapper.class);
        job.setReducerClass(WordToDocumentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.addCacheFile(new Path(args[2]).toUri());
        FileOutputFormat.setOutputPath(job, new Path(outputTempDir));

        boolean success = job.waitForCompletion(true);
        if (!success) {
            FileUtils.deleteDirectory(tempFile);
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "SecondJob");
        job2.setMapperClass(CountToWordMapper.class);
        job2.setPartitionerClass(CountToWordPartitioner.class);
        job2.setGroupingComparatorClass(GroupComparator.class);
        job2.setReducerClass(CountToWordReducer.class);
        job2.setMapOutputKeyClass(CountWord.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(outputTempDir));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        success = job2.waitForCompletion(true);
        if (!success) {
            FileUtils.deleteDirectory(tempFile);
            System.exit(1);
        }
        FileUtils.deleteDirectory(tempFile);
        System.exit(0);
    }
}
