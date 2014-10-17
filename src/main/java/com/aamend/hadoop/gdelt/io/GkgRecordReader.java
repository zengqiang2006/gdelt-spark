package com.aamend.hadoop.gdelt.io;

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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GkgRecordReader extends RecordReader<Text, Text> {

    private long start;
    private long pos;
    private long end;
    private LineRecordReader.LineReader in;
    private int maxLineLength;
    private Text key = new Text();
    private Text value = new Text();
    private Text tmp = new Text();
    private Entity root;
    private FileSplit split;

    private static final DateFormat DF_GDELT = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat DF_ISO = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        Entity sourceUrl = new Entity(String.class);
        Entity sourceUrls = new Entity("urls", "\\<UDIV\\>", new Entity[]{sourceUrl});
        Entity source = new Entity(String.class);
        Entity sources = new Entity("sources", ";", new Entity[]{source});
        Entity cameo = new Entity(Integer.class);
        Entity cameos = new Entity("cameos", ",", new Entity[]{cameo});
        Entity tone = new Entity("averageTone", Double.class);
        Entity pScore = new Entity("positiveScore", Double.class);
        Entity nScore = new Entity("negativeScore", Double.class);
        Entity polarity = new Entity("polarity", Double.class);
        Entity activityDensity = new Entity("activityDensity", Double.class);
        Entity groupDensity = new Entity("groupDensity", Double.class);
        Entity tones = new Entity("tones", ",", new Entity[]{tone, pScore, nScore, polarity, activityDensity, groupDensity});
        Entity organization = new Entity(String.class);
        Entity organizations = new Entity("organizations", ";", new Entity[]{organization});
        Entity person = new Entity(String.class);
        Entity persons = new Entity("persons", ";", new Entity[]{person});
        Entity type = new Entity("type", Integer.class);
        Entity name = new Entity("name", String.class);
        Entity country = new Entity("country", String.class);
        Entity adm = new Entity("adm", String.class);
        Entity latitude = new Entity("latitude", Double.class);
        Entity longitude = new Entity("longitude", Double.class);
        Entity feature = new Entity("feature", String.class);
        Entity location = new Entity(null, "#", new Entity[]{type, name, country, adm, latitude, longitude, feature});
        Entity locations = new Entity("locations", ";", new Entity[]{location});
        Entity theme = new Entity(String.class);
        Entity themes = new Entity("themes", ";", new Entity[]{theme});
        Entity count = new Entity(String.class);
        Entity counts = new Entity("counts", ";", new Entity[]{count});
        Entity num = new Entity("num", Integer.class);
        Entity date = new Entity("date", Date.class);
        Entity root = new Entity(null, "\t", new Entity[]{date, num, counts, themes, locations, persons, organizations, tones, cameos, sources, sourceUrls});

        this.root = root;

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

    private void addNest(JSONObject json, String token, Entity entity) {

        String[] nestedTokens = token.split(entity.getDelimiter());
        if(entity.getNestedEntities().length == 0){
            // Leaf
            JSONTuple tuple = new JSONTuple();
            tuple.setKey(entity.getName());
            tuple.setValue(token);
            Object value = convert(String.valueOf(tuple.getValue()), entity);
            if(value != null) {
                json.put(tuple.getKey(), value);
            }
        } else if (entity.getNestedEntities().length == 1){
            // Entity contains Array
            Entity nestedEntity = entity.getNestedEntities()[0];
            JSONArray array = new JSONArray();
            for (int i = 0 ; i < nestedTokens.length ; i++){
                String nestedToken = nestedTokens[i];
                if(nestedEntity.getNestedEntities().length > 0){
                    JSONObject nestedObject = new JSONObject();
                    addNest(nestedObject, nestedToken, nestedEntity);
                    array.add(nestedObject);
                } else {
                    Object value = convert(String.valueOf(nestedToken), nestedEntity);
                    if(value != null) {
                        array.add(value);
                    }
                }
            }
            if(!array.isEmpty()){
                json.put(entity.getName(), array);
            }
        } else {
            // Entity contains new Entity
            int i = 0;
            JSONObject obj = new JSONObject();
            for(String nestedToken : nestedTokens){
                try {
                    Entity nestedEntity = entity.getNestedEntities()[i];
                    if(entity.getName() == null) {
                        addNest(json, nestedToken, nestedEntity);
                    } else {
                        addNest(obj, nestedToken, nestedEntity);
                        json.put(entity.getName(), obj);
                    }
                } catch (ArrayIndexOutOfBoundsException e){
                    // Do nothing
                    // TODO: Handle bad records
                }
                i++;
            }
        }
    }

    public String parse(String line){
        JSONObject obj = new JSONObject();
        obj.put("rowId", key.toString());
        addNest(obj, line, root);
        return obj.toJSONString();
    }

    private class Entity {

        private String name;
        private String delimiter;
        private Class clazz;
        private Entity[] nestedEntities;

        private Entity(String name, String delimiter, Entity[] nestedEntities) {
            this.name = name;
            this.delimiter = delimiter;
            this.nestedEntities = nestedEntities;
            this.clazz = Object.class;
        }

        private Entity(String name, Class clazz) {
            this.name = name;
            this.delimiter = "";
            this.nestedEntities = new Entity[0];
            this.clazz = clazz;
        }

        private Entity(Class clazz) {
            this.name = "";
            this.delimiter = "";
            this.nestedEntities = new Entity[0];
            this.clazz = clazz;
        }

        public String getName() {
            return name;
        }

        public Class getClazz() {
            return clazz;
        }

        public String getDelimiter() {
            return delimiter;
        }

        public Entity[] getNestedEntities() {
            return nestedEntities;
        }
    }

    private class JSONTuple {

        private Object key;
        private Object value;

        public Object getKey() {
            return key;
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

    }
}
