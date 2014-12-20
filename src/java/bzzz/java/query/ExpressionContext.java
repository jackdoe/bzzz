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

    public int docBase;
    public int docID;
    public int global_docID;
    public int freq;
    public int doc_freq;

    public DocsAndPositionsEnum postings;
    public CollectionStatistics collection_statistics;
    public float current_score = 0f;
    public float current_decay = 1f;
    public long current_counter = 0;
    public int current_freq_left;
    public Map<String,Object> fc = new HashMap<String,Object>();
    public Map<Object,Object> local_state = new HashMap<Object,Object>();
    public Map<Integer,List<Object>> result_state = null;
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

    public void swap_local_state(Map<Object,Object> replacement) {
        local_state = replacement;
    }
    public void reset() throws IOException {
        freq = postings.freq();
        current_freq_left = freq;
        docID = postings.docID();
        global_docID = docID + docBase;
        current_score = 0f;
        current_decay = 1f;
        current_counter = 0;
    }

    public float idf(long docFreq, long numDocs) {
        return (float)(Math.log(numDocs/(double)(docFreq+1)) + 1.0);
    }

    public float tf_idf() {
        return freq * idf(doc_freq, cs_max_doc());
    }

    public float maxed_tf_idf() {
        return 1 - (1 / (float) Math.sqrt(tf_idf()));
    }

    public long cs_doc_count() { return collection_statistics.docCount(); }
    public long cs_max_doc() { return collection_statistics.maxDoc(); }
    public long cs_sum_doc_freq() { return collection_statistics.sumDocFreq(); }
    public long cs_sum_total_term_freq() { return collection_statistics.sumTotalTermFreq(); }

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
        if (--current_freq_left >= 0)
            return postings.nextPosition();
        return -1;
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
        return local_state_get(key,null);
    }

    public Object local_state_get(Object key,Object def) {
        Object r = local_state.get(key);
        if (r == null)
            return def;
        return r;
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

    public List<Object> result_state_get_for_doc(Integer doc) {
        if (result_state != null && explanation != null)
            return result_state.get(doc);
        return null;
    }

    public void result_state_append(Object val) {
        if (explanation == null)
            return;

        if (result_state == null)
            result_state = new HashMap<Integer,List<Object>>();

        List<Object> values = result_state.get(global_docID);
        if (values == null) {
            values = new ArrayList<Object>();
            values.add(val);
            result_state.put(global_docID,values);
        } else {
            values.add(val);
        }
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
        if (explanation != null)
            explanation.addDetail(new Explanation(Helper.object_to_float(s),e));
    }

    public void current_score_add(float s) { current_score += s; }
    public void current_score_add(double s) { current_score += (float) s; }
    public void current_score_add(int s) { current_score += (float) s; }
    public void current_score_add(long s) { current_score += (float) s; }
    public void current_score_add(Object s) { current_score += Helper.object_to_float(s); }

    public void current_decay_mul(float s) { current_decay *= s; }
    public void current_decay_mul(double s) { current_decay *= (float) s; }
    public void current_decay_mul(int s) { current_decay *= (float) s; }
    public void current_decay_mul(long s) { current_decay *= (float) s; }
    public void current_decay_mul(Object s) { current_decay *= Helper.object_to_float(s); }

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
        if (fba_counts == null)
            throw new IllegalStateException("<fixed-bucket-aggregation> parameter was not sent, and yet there is an attempt to fba_aggregate_into_bucket");
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
