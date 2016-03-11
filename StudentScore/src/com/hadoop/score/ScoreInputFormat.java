package com.hadoop.score;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;



/**
 * @author zp
 * ScoreInputFormat is used to specify the split and key value pair in mapper class.
 */
public class ScoreInputFormat extends FileInputFormat{

	

	

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RecordReader createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new ScoreRecordReader();
	}
	
	
	
	public static class ScoreRecordReader extends RecordReader {
		public LineReader in;
		public Text line;
		public Score lineValue;
		public Text lineKey;
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			if(in != null){
				in.close();
			}
			
		}

		@Override
		public Object getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return lineKey;
		}

		@Override
		public Object getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return lineValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit split = (FileSplit) arg0;
			Configuration job = arg1.getConfiguration();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			
			FSDataInputStream filein = fs.open(file);
			in = new LineReader(filein,job);
			line = new Text();
			lineKey = new Text();
			lineValue = new Score();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int linesize = in.readLine(line);
			if(linesize==0) return false;
			String[] pieces = line.toString().split("\\s+");
			System.out.println("size of pieces " + pieces.length);
			
			if(pieces.length != 7){
				try {
					throw new Exception("invalid record received");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			float a,b,c,d,e;
			a = Float.parseFloat(pieces[2].trim());
			b = Float.parseFloat(pieces[3].trim());
			c = Float.parseFloat(pieces[4].trim());
			d = Float.parseFloat(pieces[5].trim());
			e = Float.parseFloat(pieces[6].trim());
			
			lineKey.set(pieces[0] + "\t" + pieces[1]);
			lineValue.set(a,b,c,d,e);
			return true;
		}
		
	}

	

	
	
	
	
		
	}


