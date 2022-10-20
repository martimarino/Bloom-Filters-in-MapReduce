package it.unipi.cc.hadoop.model;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilter implements Writable {
    private IntWritable k;
    private IntWritable m;
    private BytesWritable  bf;

    public BloomFilter(){
        k = new IntWritable();
        m = new IntWritable();
        bf = new BytesWritable();
    }

    public BloomFilter(int k, int m, BitSet b){
        this.k =new IntWritable(k);
        this.m = new IntWritable(m);
        //creazione byteswritable dal bitset
        byte[] arr = b.toByteArray();
        bf = new BytesWritable();
        bf.setSize(arr.length);
        bf.set(arr, 0, arr.length);
    }

    public void setM(IntWritable m) {
        this.m = m;
    }

    public void setK(IntWritable k) {
        this.k = k;
    }

    public BitSet getBitSet(){
        return BitSet.valueOf(bf.getBytes());
    }

    public IntWritable getM() {
        return m;
    }

    public IntWritable getK() {
        return k;
    }

    public boolean find(String id) {
        int index;
        for(int i=0; i<k.get(); i++){
            index = Math.abs(MurmurHash.getInstance(MURMUR_HASH).hash(id.getBytes(), i));
            index = index % m.get();
            if (!getBitSet().get(index))
                return false;
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        bf.write(out);
        k.write(out);
        m.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        bf.readFields(in);
        k.readFields(in);
        m.readFields(in);
    }

}
