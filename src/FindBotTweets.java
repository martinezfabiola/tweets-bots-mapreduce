package org.gkfmms.java.bottweetsfinder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.lang.Math;
import java.lang.StringBuilder;

import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class FindBotTweets extends Configured implements Tool {
  private static final Pattern WORD_BOUNDARY_TAB = Pattern.compile("\\t");
  private static final Pattern WORD_BOUNDARY_SPACE = Pattern.compile("\\s+");

  private static String output = "output";
  private static String input = "input";
  private static final String tokenFrequenciesPath = "tokenFrequencies";

  private static final Logger LOG = Logger.getLogger(FindBotTweets.class);

  static enum FBTCounters {
    nonReducedNodes
  }

  public static void main(String[] args) throws Exception {
    input = args[0];
    output = args[1];
    int res = ToolRunner.run(new FindBotTweets(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

    Job job1a = Job.getInstance(conf, "Stage 1.a");
    job1a.setJarByClass(this.getClass());
    job1a.setMapperClass(Map1a.class);
    job1a.setReducerClass(Reduce1a.class);
    job1a.setOutputKeyClass(Text.class);
    job1a.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job1a, new Path(input));
    FileOutputFormat.setOutputPath(job1a, new Path(output + "/" + tokenFrequenciesPath));

    if (!job1a.waitForCompletion(true)) return 1;

    Job job1b = Job.getInstance(conf, "Stage 1.b");
    job1b.setJarByClass(this.getClass());
    job1b.setMapperClass(Map1b.class);
    job1b.setReducerClass(Reduce1b.class);
    job1b.setOutputKeyClass(Text.class);
    job1b.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1b, new Path(input));
    FileOutputFormat.setOutputPath(job1b, new Path(output + "/similarities_r"));

    if (!job1b.waitForCompletion(true)) return 1;


    Job job1c = Job.getInstance(conf, "Stage 1.c");
    job1c.setJarByClass(this.getClass());
    job1c.setMapperClass(Map1c.class);
    job1c.setReducerClass(Reduce1c.class);
    job1c.setOutputKeyClass(Text.class);
    job1c.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1c, new Path(output + "/similarities_r"));
    FileOutputFormat.setOutputPath(job1c, new Path(output + "/similarities"));

    if (!job1c.waitForCompletion(true)) return 1;


    Job job2a = Job.getInstance(conf, "Stage 2.a");
    job2a.setJarByClass(this.getClass());
    job2a.setMapperClass(Map2a.class);
    job2a.setReducerClass(Reduce2a.class);
    job2a.setOutputKeyClass(Text.class);
    job2a.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2a, new Path(output + "/similarities"));
    FileOutputFormat.setOutputPath(job2a, new Path(output + "/graph_a"));

    if (!job2a.waitForCompletion(true)) return 1;


    Job job2b = Job.getInstance(conf, "Stage 2.b");
    job2b.setJarByClass(this.getClass());
    job2b.setMapperClass(Map2b.class);
    job2b.setReducerClass(Reduce2b.class);
    job2b.setOutputKeyClass(Text.class);
    job2b.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2b, new Path(output + "/graph_a"));
    FileOutputFormat.setOutputPath(job2b, new Path(output + "/graph_b"));

    if (!job2b.waitForCompletion(true)) return 1;


    Job job2c = Job.getInstance(conf, "Stage 2.c");
    job2c.setJarByClass(this.getClass());
    job2c.setMapperClass(Map2c.class);
    job2c.setReducerClass(Reduce2c.class);
    job2c.setOutputKeyClass(Text.class);
    job2c.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2c, new Path(output + "/graph_b"));
    FileOutputFormat.setOutputPath(job2c, new Path(output + "/graph_c"));

    if (!job2c.waitForCompletion(true)) return 1;


    int iteration = 0;
    while(true) {
      Job job2d = Job.getInstance(conf, "Stage 2.d");
      job2d.setJarByClass(this.getClass());
      job2d.setMapperClass(Map2d.class);
      job2d.setReducerClass(Reduce2d.class);
      job2d.setOutputKeyClass(Text.class);
      job2d.setOutputValueClass(Text.class);

      String inGraph = iteration > 0 ? ("d" + iteration) : "c";
      String outGraph = "d" + (iteration+1);

      FileInputFormat.addInputPath(job2d, new Path(output + "/graph_" + inGraph));
      FileOutputFormat.setOutputPath(job2d, new Path(output + "/graph_" + outGraph));


      if (!job2d.waitForCompletion(true)) return 1;
      iteration++;

      long nonReducedNodes = job2d.getCounters().findCounter(FBTCounters.nonReducedNodes).getValue();
      if (nonReducedNodes == 0) break;
    }

    Job job2e = Job.getInstance(conf, "Stage 2.e");
    job2e.setJarByClass(this.getClass());
    job2e.setMapperClass(Map2e.class);
    job2e.setReducerClass(Reduce2e.class);
    job2e.setOutputKeyClass(Text.class);
    job2e.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2e, new Path(input));
    FileInputFormat.addInputPath(job2e, new Path(output + "/graph_d" + iteration));
    FileOutputFormat.setOutputPath(job2e, new Path(output + "/output"));

    if (!job2e.waitForCompletion(true)) return 1;

    return  0;
  }


  // Stage 1.a
  // ======================================================================================
  public static class Map1a extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line); // [id, tweet]

      String tweet = elems[1];
      char[] c = {'x','x','x'};
      for(int i=0; i < tweet.length()-2; i++){
        c[0] = tweet.charAt(i);
        c[1] = tweet.charAt(i+1);
        c[2] = tweet.charAt(i+2);
        String token = new String(c);
        context.write(new Text(token), one);
      }
    }
  }

  public static class Reduce1a extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }



  // Stage 1.b
  // ======================================================================================
  public static class Map1b extends Mapper<LongWritable, Text, Text, Text> {
    private Hashtable<String, Integer> hash = new Hashtable<String, Integer>();

    public void setup(Context context) throws IOException {
      FileSystem fs = FileSystem.get(context.getConfiguration());
      Path path = new Path(output + "/" + tokenFrequenciesPath);
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
      files.next();
      LocatedFileStatus file = files.next();

      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
      String line = br.readLine();
      while (line != null){
        String[] elems = WORD_BOUNDARY_TAB.split(line); // [ngram, freq]

        hash.put(elems[0], Integer.parseInt(elems[1]));
        line = br.readLine();
      }
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line); // [id, tweet]
      ArrayList<Pair<Integer, String>> ngrams = new ArrayList<Pair<Integer, String>>();
      ArrayList<Pair<Integer, String>> solve = new ArrayList<Pair<Integer, String>>();

      String tweetId = elems[0];
      String tweet = elems[1];
      char[] c = {'x','x','x'};
      for(int i=0; i < tweet.length()-2; i++) {
        c[0] = tweet.charAt(i);
        c[1] = tweet.charAt(i+1);
        c[2] = tweet.charAt(i+2);
        String token = new String(c);
        try {
          int frequency = hash.get(token);
          Pair<Integer, String> keyval  = Pair.of(frequency, token);
          ngrams.add(keyval);
        } catch (Exception e) {
          System.out.println("'" + token + "' frequency not found");
        }
      }
      int k = 5;
      for(int i=0; i<k; i++){
        Integer maximum = ngrams.get(0).getKey();
        Pair<Integer,String> newPair = ngrams.get(0);
        for(Pair<Integer,String> pair : ngrams){
          if(maximum < pair.getKey()){
            maximum = pair.getKey();
            newPair = pair;
          }
        }
        solve.add(newPair);
        ngrams.remove(newPair);
      }
      for(Pair<Integer,String> pair : solve){
        context.write(new Text(pair.getValue()), lineText);
      }
    }
  }


  public static class Reduce1b extends Reducer<Text, Text, DoubleWritable, Text> {
    private JaroWinkler jw = new JaroWinkler();

    private double sim(String a, String b){
      return jw.similarity(a, b);
    }

    public void reduce(Text ngram, Iterable<Text> tuplesI, Context context)
        throws IOException, InterruptedException {
      ArrayList<String> tuples = new ArrayList<String>();
      for(Text text : tuplesI) {
        tuples.add(text.toString());
      }
      int len = tuples.size();
      for(int i=0; i<len; i++) {
        String[] tweet1 = WORD_BOUNDARY_TAB.split(tuples.get(i));
        for(int j=i+1; j<len; j++) {
          String[] tweet2 = WORD_BOUNDARY_TAB.split(tuples.get(j));
          double similarity = sim(tweet1[1], tweet2[1]);
          if (similarity > jw.getThreshold())
            context.write(new DoubleWritable(similarity), new Text(String.join("\t", tweet1[0], tweet2[0])));
        }
      }
    }
  }


  // Stage 1.c
  // ======================================================================================
  public static class Map1c extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line);
      switch(elems[1].compareTo(elems[2])){
        case 1:
          context.write(new Text(String.join("\t", elems[1], elems[2])), new Text());
          break;
        case -1:
          context.write(new Text(String.join("\t", elems[2], elems[1])), new Text());
          break;
      }
    }
  }
  public static class Reduce1c extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text arcText, Iterable<Text> nothing, Context context)
        throws IOException, InterruptedException {
      String arc = arcText.toString();
      String[] nodes = WORD_BOUNDARY_TAB.split(arc);
      context.write(new Text(nodes[0]), new Text(nodes[1]));
    }
  }

  // Stage 2.a
  // ======================================================================================
  public static class Map2a extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line);
      context.write(new Text(elems[0]), new Text(elems[1]));
      context.write(new Text(elems[1]), new Text(elems[0]));
    }
  }
  public static class Reduce2a extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text id, Iterable<Text> adjs, Context context)
        throws IOException, InterruptedException {
      int w = 0;
      StringBuilder adjList = new StringBuilder();
      for(Text adjText : adjs){
        String adj = adjText.toString();
        adjList.append(adj);
        adjList.append('\t');
        w++;
      }
      context.write(id, new Text(String.join("\t", String.valueOf(w), adjList.toString())));
    }
  }

  // Stage 2.b
  // ======================================================================================
  public static class Map2b extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line);
      Text id = new Text(elems[0]),
           w = new Text('$' + elems[1]);

      context.write(id, w);
      for(int i=2; i < elems.length; i++){
        context.write(new Text(elems[i]), id);
      }
    }
  }
  public static class Reduce2b extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text idText, Iterable<Text> radjs, Context context)
        throws IOException, InterruptedException {
      String w = "";
      ArrayList<String> ids = new ArrayList<String>();
      for(Text radjText : radjs){
        String radj = radjText.toString();
        if(radj.charAt(0) == '$'){
          w = radj.substring(1);
        } else ids.add(radj);
      }
      String id = idText.toString();
      for(String radj : ids){
        context.write(new Text(radj), new Text(String.join("\t", id, w)));
      }
    }
  }

  // Stage 2.c
  // ======================================================================================
  public static class Map2c extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line);
      context.write(new Text(elems[0]), new Text(String.join("\t", elems[1], elems[2])));
    }
  }
  public static class Reduce2c extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text id, Iterable<Text> adjs, Context context)
        throws IOException, InterruptedException {
      StringBuilder list = new StringBuilder();
      int w = 0;
      for(Text adjText : adjs){ // id and w in turns
        String adj = adjText.toString();
        list.append(adj);
        list.append("\t");
        w++;
      }
      context.write(id, new Text(String.join("\t", String.valueOf(w), list.toString())));
    }
  }

  // Stage 2.d
  // ======================================================================================
  public static class Map2d extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line);
      String id = elems[0];
      int w = Integer.parseInt(elems[1]);
      elems = Arrays.copyOfRange(elems, 2, elems.length);

      // We need the graph in the reduce
      context.write(new Text(id), new Text(String.join("\t", elems)));
      if (elems.length < 2 && elems[0].equals("F")) {
        context.write(new Text(id), new Text("=" + String.valueOf(w)));
        return;
      }

      // We get the max node
      int maxW = w;
      String maxId = id;
      for(int i=0; i<elems.length; i+=2) {
        int thisW = Integer.parseInt(elems[i+1]);
        if (thisW > maxW) {
          maxW = thisW;
          maxId = elems[i];
        }
      }
      context.write(new Text(maxId), new Text("+" + String.valueOf(maxW)));

      // We order to remove the non-maximum nodes
      for(int i=0; i<elems.length; i+=2){
        if (!maxId.equals(elems[i])) {
          context.write(new Text(id), new Text("-" + elems[i]));
        }
      }
    }
  }

  public static class Reduce2d extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text id, Iterable<Text> adjs, Context context)
        throws IOException, InterruptedException {
      Hashtable<String, Boolean> toDel = new Hashtable<String, Boolean>();
      ArrayList<String[]> cache = new ArrayList<String[]>();
      String thisW = "";
      boolean isMax = false, 
              isFinal = false;

      for(Text lineText : adjs){ // id and w in turns
        String line = lineText.toString();
        String[] elems = WORD_BOUNDARY_TAB.split(line);
        switch(elems[0].charAt(0)) {
          case '=':
            isFinal = true;
          case '+':
            isMax = true;
            thisW = elems[0].substring(1);
            break;
          case '-':
            toDel.put(elems[0].substring(1), true);
            break;
          default:
            cache.add(elems);
        }
      }
      if (isFinal) {
        context.write(id, new Text(thisW + "\tF"));
        return;
      }
      if (!isMax) return;
      StringBuilder cleanElems = new StringBuilder();
      cleanElems.append(String.valueOf(thisW));
      boolean pruning = false;
      for(String[] elems : cache){
        // String[] elems = WORD_BOUNDARY_TAB.split(line);
        for(int i=0; i<elems.length; i+=2){
          if(!toDel.containsKey(elems[i])) {
            pruning = true;
            cleanElems.append("\t");
            cleanElems.append(elems[i]);
            cleanElems.append("\t");
            cleanElems.append(elems[i+1]);
          }
        }
      }
      if (pruning) {
				context.getCounter(FBTCounters.nonReducedNodes).increment(1L);
        context.write(id, new Text(cleanElems.toString()));
      } else {
        cleanElems.append("\t").append("F");
        context.write(id, new Text(cleanElems.toString()));
      }
    }
  }


  // Stage 2.e
  // ======================================================================================
  public static class Map2e extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] elems = WORD_BOUNDARY_TAB.split(line);
      if (elems.length == 3)
        context.write(new Text(elems[0]), new Text(""));
      else
        context.write(new Text(elems[0]), new Text(elems[1]));
    }
  }
  public static class Reduce2e extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text id, Iterable<Text> list, Context context)
        throws IOException, InterruptedException {
      int c = 0;
      String tweet = "";
      for(Text tweetText : list){
        c++;
        String maybeTweet = tweetText.toString();
        if(maybeTweet.length() > 0){
          tweet = maybeTweet;
        }
      }
      if (c==2) context.write(new Text(id), new Text(tweet));
    }
  }
}

