package it.unipi.cc.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.util.hash.Hash;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilter implements Writable {
    int k;
    int m;
    BitSet bf;
    private Hash murmurHashInstance;

    public BloomFilter(int k, int m){
        this.k = k;
        this.m = m;
        bf = new BitSet(m);
        murmurHashInstance = Hash.getInstance(MURMUR_HASH);
    }

    public boolean insert(String id) {
        int index;
        for(int i = 0; i < k; i++)
        {
            index = (murmurHashInstance.hash(id.getBytes(), i)) % m;
            bf.set(index, true);
        }
        return true;
    }

    public boolean find(String id) {
        int index;
        for(int i=0; i<k; i++){
            index = murmurHashInstance.hash(id.getBytes(), i);
            index = index % m;
            if (!bf.get(index))
                return false;
        }
        return true;
    }

    public void or(BitSet next_bf) {
        for (int i=0; i < this.m; i++)
            bf.set(i, bf.get(i) || next_bf.get(i));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
