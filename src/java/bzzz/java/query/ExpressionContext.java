package bzzz.java.query;
import org.apache.lucene.index.*;
import org.apache.lucene.search.FieldCache.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import clojure.lang.Keyword;
import java.io.*;
import java.util.*;

public class ExpressionContext {
    public Explanation explanation = null;
    public int docID;
    public int freq;
    public DocsAndPositionsEnum postings;


    public Map<String,Object> fc = new HashMap<String,Object>();
    public Map<Object,Object> local_state = new HashMap<Object,Object>();
    public Map<Object,Object> global_state;

    // "fixed_bucket_aggregations": [
    //   { "name": "distance", "buckets": 6 }
    //   { "name": "rating", "buckets": 10 }
    // ]
    public List<Map<Object,Object>> fba_settings = null;
    public int[] fba_counts = null;
    int fba_max_buckets_per_aggregation = 0;
    public static final Keyword FBA_KW_NAME = Keyword.intern(null, "name");
    public static final Keyword FBA_KW_BUCKETS = Keyword.intern(null, "buckets");

    public ExpressionContext(Map<Object,Object> global_state,List<Map<Object,Object>> fba_settings) throws Exception {
        this.global_state = global_state;
        this.fba_settings = fba_settings;
        if (fba_settings != null)
            fba_initialize();
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
    // FIXED_BUCKET_AGGREGATION
    public Object fba_read_key(Map<Object,Object> item, String x, Keyword fallback) {
        Object result = item.get(x);
        if (result != null)
            return result;
        return item.get(fallback);
    }
    public void fba_initialize() throws Exception {
        for (Map<Object,Object> item : fba_settings) {
            String name = (String) fba_read_key(item,"name",FBA_KW_NAME);
            Object buckets_raw = fba_read_key(item,"buckets",FBA_KW_BUCKETS);
            int buckets = 0;
            if (buckets_raw instanceof String)
                buckets = Integer.parseInt((String) buckets_raw);
            else if (buckets_raw instanceof Integer)
                buckets = (Integer) buckets_raw;
            else if (buckets_raw instanceof Long)
                buckets = ((Long) buckets_raw).intValue();
            else
                throw new Exception("unknown object type, expecting string/integer" + item);

            if (buckets > fba_max_buckets_per_aggregation)
                fba_max_buckets_per_aggregation = buckets;
        }
        if (fba_max_buckets_per_aggregation > 0)
            fba_counts = new int[fba_max_buckets_per_aggregation * fba_settings.size()];
    }

    public void fba_aggregate_into_bucket(int aggregation_index, int bucket) {
        fba_aggregate_into_bucket(aggregation_index,bucket,1);
    }

    public void fba_aggregate_into_bucket(int aggregation_index, int bucket, int increment) {
        fba_counts[(aggregation_index * fba_max_buckets_per_aggregation) + bucket] += increment;
    }

    public Map<String,List<Map<String,Integer>>> fba_get_results() {
        Map<String,List<Map<String,Integer>>> result = new HashMap<String,List<Map<String,Integer>>>();
        int aggregation_index = 0;
        if (fba_counts != null) {
            for (Map<Object,Object> item : fba_settings) {
                List<Map<String,Integer>> value = new ArrayList<Map<String,Integer>>();
                for (int j = 0; j < fba_max_buckets_per_aggregation; j++) {
                    int n = fba_counts[(aggregation_index * fba_max_buckets_per_aggregation) + j];
                    if (n > 0) {
                        Map<String,Integer> label_and_value = new HashMap<String,Integer>();
                        label_and_value.put("label",j);
                        label_and_value.put("count",n);
                        value.add(label_and_value);
                    }
                }
                if (value.size() > 0) {
                    Collections.sort(value, new Comparator<Map<String,Integer>>() {
                            @Override
                            public int compare(Map<String,Integer> a, Map<String,Integer> b) {
                                Integer aa = a.get("count");
                                Integer bb = b.get("count");
                                if (aa == null)
                                    aa = 0;
                                if (bb == null)
                                    bb = 0;
                                return bb.compareTo(aa);
                            }
                        });
                    result.put((String) fba_read_key(item,"name",FBA_KW_NAME), value);
                }
                aggregation_index++;
            }
        }
        return result;
    }
    // \FIXED_BUCKET_AGGREGATION
}
