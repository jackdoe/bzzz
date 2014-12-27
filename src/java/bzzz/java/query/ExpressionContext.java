package bzzz.java.query;
import org.apache.lucene.index.*;
import org.apache.lucene.search.FieldCache.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.symmetric.*;

public class ExpressionContext {
    public Explanation explanation = null;

    public int doc_id;
    public int global_doc_id;
    public Map<Term,CollectionStatistics>  collection_statistics = new HashMap<Term,CollectionStatistics>();
    public List<PerTerm> per_term = new ArrayList<PerTerm>();
    public float current_score = 0f;
    public long current_counter = 0;
    public int total_term_count = 0;
    public Map<Object,Object> local_state = new HashMap<Object,Object>(1000);
    public Map<String,Object> fc = new HashMap<String,Object>();
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

    public EWAHCompressedBitmap[] context_collect_bitmaps() throws IOException {
        EWAHCompressedBitmap[] maps = new EWAHCompressedBitmap[per_term.size()];
        for (int i = 0; i < per_term.size(); i++) {
            maps[i] = per_term.get(i).bitmap_from_payload();
        }
        return maps;
    }

    public EWAHCompressedBitmap context_anded_bitmaps() throws IOException {
        return EWAHCompressedBitmap.and(context_collect_bitmaps());
    }

    public void reset() throws IOException {
        current_score = 0f;
        current_counter = 0;
    }

    public ExpressionContext(Map<Object,Object> global_state,List<Map<Object,Object>> fba_settings) throws Exception {
        this.global_state = global_state;
        this.fba_settings = fba_settings;
        if (fba_settings != null)
            fba_initialize();
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
    public int payload_get_int() throws IOException {
        if (total_term_count > 1)
            throw new IllegalStateException("cannot use context.payload_get_int() with more than 1 term, use context.per_term.get(N).payload_get_int() instead");
        return Helper.decode_int_payload(per_term.get(0).payload());
    }


    public float sum_maxed_tf_idf() throws IOException {
        float s = 0f;
        for (PerTerm pt : per_term) {
            if (pt.postings.docID() == doc_id)
                s += pt.maxed_tf_idf();
        }
        return s;
    }

    public int matching() {
        int matching = 0;
        for (PerTerm pt : per_term) {
            if (pt.postings.docID() == doc_id)
                matching++;
        }
        return matching;
    }
    public int missing() {
        return total_term_count - matching();
    }

    public float fc_get_float(String name) {
        return fc_get_float(name,doc_id);
    }
    public int fc_get_integer(String name) {
        return fc_get_integer(name,doc_id);
    }
    public int fc_get_int(String name) {
        return fc_get_integer(name,doc_id);
    }
    public double fc_get_double(String name) {
        return fc_get_double(name,doc_id);
    }
    public long fc_get_long(String name) {
        return fc_get_long(name,doc_id);
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

        List<Object> values = result_state.get(global_doc_id);
        if (values == null) {
            values = new ArrayList<Object>();
            values.add(val);
            result_state.put(global_doc_id,values);
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

    public void current_counter_set(int s) { current_counter = s; }
    public void current_counter_inc() { current_counter++; }
    public void current_counter_set(long s) { current_counter = (int) s; }

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

    public static final class PerTerm {
        public DocsAndPositionsEnum postings;
        public CollectionStatistics per_term_collection_statistics;
        public int doc_freq;
        public EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        public int token_position;

        public float idf(long docFreq, long numDocs) {
            return (float)(Math.log(numDocs/(double)(docFreq+1)) + 1.0);
        }
        public float tf_idf() throws IOException {
            return postings.freq() * idf(doc_freq, cs_max_doc());
        }
        public float maxed_tf_idf() throws IOException {
            return 1 - (1 / (float) Math.sqrt(tf_idf()));
        }
        public BytesRef payload() throws IOException {
            return postings.getPayload();
        }
        public int postings_next_position() throws IOException {
            return postings.nextPosition();
        }
        public int payload_get_int() throws IOException {
            return Helper.decode_int_payload(payload());
        }
        public EWAHCompressedBitmap bitmap_from_payload() throws IOException{
            bitmap.clear();
            BytesRef p = payload();
            if (p != null) {
                ByteArrayInputStream bis = new ByteArrayInputStream(p.bytes,p.offset,p.bytes.length);
                bitmap.readExternal(new ObjectInputStream(bis));
            }
            return bitmap;
        }
        public long cs_doc_count() { return per_term_collection_statistics.docCount(); }
        public long cs_max_doc() { return per_term_collection_statistics.maxDoc(); }
        public long cs_sum_doc_freq() { return per_term_collection_statistics.sumDocFreq(); }
        public long cs_sum_total_term_freq() { return per_term_collection_statistics.sumTotalTermFreq(); }
    }
}
