package com.hadoop.score;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class CountScore extends Configured implements Tool {
		
	public static class ScoreMapper extends Mapper< Text, Score, Text, Score> {
		
		public void map(Text key,Score value,Context context) throws IOException, InterruptedException {
			context.write(key,value);
		
		}
	}
	
	public static class ScoreReducer extends Reducer< Text, Score, Text, Text > {
			Text score = new Text();
			public void reduce(Text key, Iterable<Score> values, Context context) throws IOException, InterruptedException{
				float totalscore = 0.0f;
				float averagescore = 0.0f;
				for(Score v:values){
				 totalscore += v.getChinese() + v.getMath() + v.getEnglish() + v.getChemistry() + v.getPhysics();
				
				averagescore += totalscore/5;
				}
				
				score.set(totalscore + "\t" + averagescore);
				context.write(key,score);
			}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();//��ȡ�����ļ�
	        
	        Path mypath = new Path(arg0[1]);
	        FileSystem hdfs = mypath.getFileSystem(conf);//�������·��
	        if (hdfs.isDirectory(mypath)) {
	            hdfs.delete(mypath, true);
	        }
	        
	        Job job = new Job(conf, "CountScore");//�½�����
	        job.setJarByClass(CountScore.class);//��������
	        
	        FileInputFormat.addInputPath(job, new Path(arg0[0]));// ����·��
	        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));// ���·��
	        
	        job.setMapperClass(ScoreMapper.class);// Mapper
	        job.setReducerClass(ScoreReducer.class);// Reducer
	        
	        job.setMapOutputKeyClass(Text.class);// Mapper key�������
	        job.setMapOutputValueClass(Score.class);// Mapper value�������
	                
	        job.setInputFormatClass(ScoreInputFormat.class);//�����Զ��������ʽ
	        
	        job.waitForCompletion(true);   
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		String[] arg0 = {
				"hdfs://zhengpeng:9000/score/score.txt.utf8",
                "hdfs://zhengpeng:9000/score/score-out/" 
		};
		
		 int ec = ToolRunner.run(new Configuration(), new CountScore(), arg0);
        System.exit(ec);
	}
	
	
	
}

	
