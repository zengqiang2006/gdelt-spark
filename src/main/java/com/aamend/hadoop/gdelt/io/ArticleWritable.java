package com.aamend.hadoop.gdelt.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArticleWritable implements Writable {

    private String title;
    private String content;

    public ArticleWritable() {
    }

    public ArticleWritable(String title, String content) {
        this.title = title == null ? "" : title;
        this.content = content == null ? "" : content;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title == null ? "" : title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content == null ? "" : content;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(title);
        out.writeUTF(content);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.content = in.readUTF();
    }

    @Override
    public String toString() {
        return "ArticleWritable{" +
                "title='" + title + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
