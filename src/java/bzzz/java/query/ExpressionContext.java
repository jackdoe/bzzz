package bzzz.java.query;
import org.apache.lucene.index.*;
import org.apache.lucene.search.FieldCache.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import java.io.*;
import java.util.*;

public class ExpressionContext {
    public Map<Object,Object> global_state;

    public Explanation explanation = null;
    public int docID;
    public Map<String,Object> fc = new HashMap<String,Object>();
    public Map<Object,Object> local_state = new HashMap<Object,Object>();
    public int freq;

    public DocsAndPositionsEnum postings;

    public ExpressionContext(Map<Object,Object> global_state) {
        this.global_state = global_state;
    }

    public int payload_get_int() throws IOException {
        return Helper.decode_int_payload(payload());
    }

    public BytesRef payload() throws IOException {
        return postings.getPayload();
    }

    public int postings_next_position() throws IOException {
        return postings.nextPosition();
    }

    public void fill_field_cache(AtomicReader r, String[] field_cache_req) throws IOException {
        if (field_cache_req != null) {
            for (String name : field_cache_req) {
                if (name.indexOf("_int") != -1)
                    fc.put(name,FieldCache.DEFAULT.getInts(r,name,false));
                else if (name.indexOf("_long") != -1)
                    fc.put(name,FieldCache.DEFAULT.getLongs(r,name,false));
                else if (name.indexOf("_float") != -1)
                    fc.put(name,FieldCache.DEFAULT.getFloats(r,name,false));
                else if (name.indexOf("_double") != -1)
                    fc.put(name,FieldCache.DEFAULT.getDoubles(r,name,false));
                else
                    throw new IOException(name + " can only get field cache for _int|_long|_float|_double");
            }
        }
    }

    public float fc_get_float(String name) {
        return fc_get_float(name,docID);
    }
    public int fc_get_integer(String name) {
        return fc_get_integer(name,docID);
    }
    public int fc_get_int(String name) {
        return fc_get_integer(name,docID);
    }
    public double fc_get_double(String name) {
        return fc_get_double(name,docID);
    }
    public long fc_get_long(String name) {
        return fc_get_long(name,docID);
    }

    public float fc_get_float(String name,int doc) {
        Floats f = (Floats) fc.get(name);
        return f.get(doc);
    }
    public int fc_get_integer(String name,int doc) {
        Ints f = (Ints) fc.get(name);
        return f.get(doc);
    }
    public int fc_get_int(String name,int doc) {
        return fc_get_integer(name,doc);
    }
    public long fc_get_long(String name,int doc) {
        Longs f = (Longs) fc.get(name);
        return f.get(doc);
    }
    public double fc_get_double(String name,int doc) {
        Doubles f = (Doubles) fc.get(name);
        return f.get(doc);
    }

    public Object local_state_get(Object key) {
        return local_state.get(key);
    }
    public Object local_state_set(Object key, Object val) {
        return local_state.put(key,val);
    }

    public Object global_state_get(Object key) {
        return global_state.get(key);
    }
    public Object global_state_set(Object key, Object val) {
        return global_state.put(key,val);
    }

    public void explanation_add(float s, String e) {
        if (explanation != null)
            explanation.addDetail(new Explanation(s,e));
    }

    public void explanation_add(double s, String e) {
        if (explanation != null)
            explanation.addDetail(new Explanation((float)s,e));
    }

    public void explanation_add(int s, String e) {
        if (explanation != null)
            explanation.addDetail(new Explanation((float) s,e));
    }

    public void explanation_add(long s, String e) {
        if (explanation != null)
            explanation.addDetail(new Explanation((float) s,e));
    }

    public void explanation_add(Object s, String e) {
        if (explanation != null) {
            if (s instanceof Integer)
                explanation_add(((Integer) s).floatValue(), e);
            else if (s instanceof Long)
                explanation_add(((Long) s).floatValue(), e);
            else
                explanation_add((Float) s, e);
        }
    }
}
