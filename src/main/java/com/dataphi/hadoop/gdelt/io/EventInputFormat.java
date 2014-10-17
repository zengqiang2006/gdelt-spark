package com.dataphi.hadoop.gdelt.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class EventInputFormat extends FileInputFormat<Text,Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new EventRecordReader();
    }
}
