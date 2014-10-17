package com.dataphi.hadoop.gdelt.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EventRecordReader extends RecordReader<Text, Text> {

    private long start;
    private long pos;
    private long end;
    private LineRecordReader.LineReader in;
    private int maxLineLength;
    private Text key = new Text();
    private Text value = new Text();
    private Text tmp = new Text();
    private List<Entity> entities;
    private FileSplit split;

    private static final String DELIMITER = "\t";
    private static final DateFormat DF_GDELT = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat DF_ISO = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        entities = new ArrayList<Entity>();

        // Actor1, Actor2
        List<Entity> codes = new ArrayList<Entity>();
        codes.add(new Entity("code", String.class));
        codes.add(new Entity("name", String.class));
        codes.add(new Entity("countryCode", String.class));
        codes.add(new Entity("knownGroupCode", String.class));
        codes.add(new Entity("ethnicCode", String.class));
        codes.add(new Entity("religion1Code", String.class));
        codes.add(new Entity("religion2Code", String.class));
        codes.add(new Entity("type1Code", String.class));
        codes.add(new Entity("type2Code", String.class));
        codes.add(new Entity("type3Code", String.class));

        // Actor1, Actor2, Event
        List<Entity> geo = new ArrayList<Entity>();
        geo.add(new Entity("type", Integer.class));
        geo.add(new Entity("name", String.class));
        geo.add(new Entity("countryCode", String.class));
        geo.add(new Entity("adm1Code", String.class));
        geo.add(new Entity("lat", Double.class));
        geo.add(new Entity("long", Double.class));
        geo.add(new Entity("featureId", Integer.class));

        // Actor1, Actor2
        entities.add(new Entity("cameo", Integer.class));
        entities.add(new Entity("day", Date.class));
        entities.add(new Entity("month", Integer.class));
        entities.add(new Entity("year", Integer.class));
        entities.add(new Entity("fracDate", Double.class));
        entities.add(new Entity("actor1Code", codes));
        entities.add(new Entity("actor2Code", codes));

        // Common
        entities.add(new Entity("isRoot", Boolean.class));
        entities.add(new Entity("eventCode", String.class));
        entities.add(new Entity("eventBaseCode", String.class));
        entities.add(new Entity("eventRootCode", String.class));
        entities.add(new Entity("quadClass", Integer.class));
        entities.add(new Entity("goldstein", Double.class));
        entities.add(new Entity("numMentions", Integer.class));
        entities.add(new Entity("numSources", Integer.class));
        entities.add(new Entity("numArticles", Integer.class));
        entities.add(new Entity("avgTone", Double.class));
        entities.add(new Entity("actor1Geo", geo));
        entities.add(new Entity("actor2Geo", geo));
        entities.add(new Entity("eventGeo", geo));
        entities.add(new Entity("dateAdded", Integer.class));
        entities.add(new Entity("url", String.class));

        FileSplit split = (FileSplit) genericSplit;
        this.split = split;

        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            --start;
            fileIn.seek(start);
        }

        in = new LineRecordReader.LineReader(fileIn, job);
        if (skipFirstLine) {
            start += in.readLine(tmp, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }

        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        // Unique key for record is "fileSplit + offset"
        key.set(MD5Hash.digest(split.toString() + "-" + pos).toString());
        int newSize = 0;
        while (pos < end) {

            // Let Hadoop LineReader consume the line into tmp text...
            newSize = in.readLine(tmp,
                    maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                            maxLineLength));

            // ... Take the result and feed my parser
            // tmp is the raw data
            // value will be the expected JSON object
            value.set(parse(tmp.toString()));
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }
        }

        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    private static Object convert(String token, Entity entity) {
        Class clazz = entity.getClazz();
        if(clazz == null){
            throw new IllegalArgumentException("Class not supplied for token " + entity.getName());
        }
        if(token == null || token.equals("")){
            return null;
        }
        try {
            if (clazz.equals(Integer.class)) {
                return Integer.parseInt(token);
            } else if (clazz.equals(Double.class)) {
                return Double.parseDouble(token);
            } else if (clazz.equals(Date.class)) {
                return DF_ISO.format(DF_GDELT.parse(token));
            } else if (clazz.equals(Boolean.class)) {
                return token.equals("1");
            } else {
                return token;
            }
        } catch (Exception e) {
            return null;
        }
    }

    private String parse(String line){

        String[] tokens = line.split(DELIMITER);
        for(int i = 0 ; i < tokens.length ; i++){
            int j = 0;
            for (Entity entity : entities){
                if(entity.getNestedEntities().size() > 0) {
                    for (Entity nestedEntity : entity.getNestedEntities()) {
                        if(i == j){
                            // Found our nestedEntity
                            nestedEntity.setValue(convert(tokens[i], nestedEntity));
                        }
                        j++;
                    }
                } else {
                    if(i == j){
                        // Found our leaf
                        entity.setValue(convert(tokens[i], entity));
                    }
                    j++;
                }
            }
        }

        JSONObject json = new JSONObject();
        for (Entity entity : entities){
            if(entity.getNestedEntities().size() > 0) {
                boolean atLeastOne = false;
                JSONObject nestedObject = new JSONObject();
                for (Entity nestedEntity : entity.getNestedEntities()) {
                    if(nestedEntity.getValue() != null) {
                        atLeastOne = true;
                        nestedObject.put(nestedEntity.getName(), nestedEntity.getValue());
                    }
                }
                if(atLeastOne){
                    json.put(entity.getName(), nestedObject);
                }
            } else {
                if(entity.getValue() != null){
                    json.put(entity.getName(), entity.getValue());
                }
            }
        }
        return json.toJSONString();
    }

    private class Entity {

        private Object value;
        private String name;
        private Class clazz;
        private List<Entity> nestedEntities;

        // Entity containing nested entities
        private Entity(String name, List<Entity> nestedEntities) {
            this.name = name;
            this.nestedEntities = nestedEntities;
            this.clazz = Object.class;
        }

        // Leaf
        private Entity(String name, Class clazz) {
            this.name = name;
            this.nestedEntities = new ArrayList<Entity>(0);
            this.clazz = clazz;
        }

        public String getName() {
            return name;
        }

        public Class getClazz() {
            return clazz;
        }

        public List<Entity> getNestedEntities() {
            return nestedEntities;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }
}
