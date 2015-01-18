package bzzz.java.query;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import java.io.*;
import java.util.*;
import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.IFn;
import clojure.lang.Compiler;
import clojure.lang.Keyword;
import clojure.lang.APersistentMap;
import clojure.lang.PersistentHashMap;
import java.io.StringReader;
import java.lang.StringBuilder;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;
import bzzz.java.query.ExpressionContext;
import bzzz.java.query.ExpressionContext.*;

public class TermPayloadClojureScoreQuery extends Query {
    // THIS QUERY IS NOT THREAD SAFE! at the moment this is ok because of the way we create one (future) per shard
    // and the query is created in the (future) thread itself.

    public static int GLOBAL_STATE_CAPACITY = 100000; // TODO: make this a parameter

    public static Map<Object,Object> EXPR_GLOBAL_STATE = new Builder<Object,Object>().maximumWeightedCapacity(GLOBAL_STATE_CAPACITY).build();
    public static APersistentMap EXPR_GLOBAL_STATE_RO = PersistentHashMap.EMPTY;
    final public List<Term> terms;
    public String expr;
    public String[] field_cache_req;
    // "fixed_bucket_aggregations": [
    //   { "name": "distance", "buckets": 6 }
    //   { "name": "rating", "buckets": 10 }
    // ]


    public IFn clj_expr;
    public Object args;

    public Map<Integer,List<Object>> result_state = new HashMap<Integer,List<Object>>();

    public List<Map<Object,Object>> fba_settings = null;
    public int[] fba_counts = null;
    int fba_max_buckets_per_aggregation = 0;
    public Map<Object,Object> local_state = new HashMap<Object,Object>(1000);

    public static final Keyword FBA_KW_NAME = Keyword.intern(null, "name");
    public static final Keyword FBA_KW_BUCKETS = Keyword.intern(null, "buckets");

    public TermPayloadClojureScoreQuery(List <Term>terms, IFn expr, Object args,String[] field_cache_req,List<Map<Object,Object>> fba_settings) throws Exception {
        this.terms = terms;
        this.field_cache_req = field_cache_req;
        this.clj_expr = expr;
        this.args = args;
        this.fba_settings = fba_settings;
        if (fba_settings != null)
            fba_initialize();
    }

    public List<Object> result_state_get_for_doc(Integer doc) {
        return result_state.get(doc);
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


    public static void replace_expr_global_state_ro(APersistentMap replacement) {
        EXPR_GLOBAL_STATE_RO = replacement;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Term t : terms) {
            sb.append("[");
            sb.append(t.toString());
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher) throws IOException {
        final Query query = this;
        return new Weight() {
            public final Weight weight = this;
            @Override
            public String toString() { return "clojure-payload-score-weight(" + query.toString() + ")"; }
            @Override
            public Query getQuery() { return query; }
            @Override
            public float getValueForNormalization() throws IOException { return 1; }
            @Override
            public void normalize(float queryNorm, float topLevelBoost) {}
            @Override
            public ContextScorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                ContextScorer s = new ContextScorer(this, context,acceptDocs,field_cache_req,terms,args,clj_expr);
                for (Term t : terms)
                    s.clj_context.collection_statistics.put(t,searcher.collectionStatistics(t.field()));
                s.clj_context.fba_counts = fba_counts;
                s.clj_context.fba_max_buckets_per_aggregation = fba_max_buckets_per_aggregation;
                s.clj_context.result_state = result_state;
                s.clj_context.local_state = local_state;
                return s;
            }
            @Override
            public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
                ContextScorer s = scorer(context,context.reader().getLiveDocs());
                if (s != null && s.advance(doc) == doc ) {
                    s.clj_context.explanation = null;
                    s.clj_context.explanation = new Explanation();
                    float score = s.score();
                    s.clj_context.explanation.setValue(score);
                    s.clj_context.explanation.setDescription("result of: " + query.toString() + " @ " + expr);
                    return s.clj_context.explanation;
                }
                return new Explanation(0f,"no matching");
            }
        };
    }

    public static final class ContextScorer extends Scorer {
        public ExpressionContext clj_context = null;
        final List<Scorer> scorers = new ArrayList<Scorer>();
        int docBase;
        Object args;
        Weight weight;
        public IFn clj_expr;
        public ContextScorer(Weight weight, AtomicReaderContext context, Bits acceptDocs,String[] field_cache_req, List<Term>terms, Object args,IFn clj_expr) throws IOException {
            super(weight);
            this.weight = weight;
            this.clj_expr = clj_expr;
            this.args = args;
            try {
                clj_context = new ExpressionContext(EXPR_GLOBAL_STATE,EXPR_GLOBAL_STATE_RO);
                clj_context.total_term_count = terms.size();
                clj_context.fill_field_cache(context.reader(),field_cache_req);
                clj_context.per_term.clear();
            } catch(Exception e) {
                throw new IllegalStateException(e);
            }
            for (int i = 0; i < terms.size(); i++) {
                Scorer s = createSubScorer(context,acceptDocs,terms.get(i),i);
                if (s != null)
                    scorers.add(s);
            }
            docBase = context.docBase;
            this.args = args;
        }

        public int doc_id = -1;
        @Override
        public String toString() { return "scorer(" + weight.getQuery().toString() + ")"; }
        @Override
        public int docID() { return doc_id; }
        @Override
        public int freq() throws IOException {
            int f = 0;
            for (Scorer s : scorers)
                f += s.freq();
            return f;
        }
        @Override
        public long cost() {
            int c = 0;
            for (Scorer s : scorers)
                c += s.cost();
            return c;
        }
        @Override
        public int nextDoc() throws IOException {
            int newDoc = NO_MORE_DOCS;
            for (Scorer sub : scorers) {
                int curDoc = sub.docID();
                if (curDoc==doc_id) curDoc = sub.nextDoc();
                if (curDoc < newDoc) newDoc = curDoc;
            }
            this.doc_id = newDoc;
            return newDoc;
        }
        @Override
        public int advance(int target) throws IOException {
            int newDoc = NO_MORE_DOCS;
            for (Scorer sub : scorers) {
                int curDoc = sub.docID();
                if (curDoc < target) curDoc = sub.advance(target);
                if (curDoc < newDoc) newDoc = curDoc;
            }
            this.doc_id = newDoc;
            return newDoc;
        }
        @Override
        public float score() throws IOException {
            clj_context.reset();
            clj_context.doc_id = doc_id;
            clj_context.global_doc_id = doc_id + docBase;
            if (args == null)
                return (float) clj_expr.invoke(clj_context);
            return (float) clj_expr.invoke(clj_context,args);
        }

        public Scorer createSubScorer(AtomicReaderContext context, Bits acceptDocs, Term term,int pos) throws IOException {
            Terms terms = context.reader().terms(term.field());
            if (terms == null)
                return null;

            final TermsEnum termsEnum = terms.iterator(null);
            if (!termsEnum.seekExact(term.bytes()))
                return null;

            final Weight weight = this.weight;
            final TermState state = termsEnum.termState();
            termsEnum.seekExact(term.bytes(), state);
            final DocsAndPositionsEnum postings = termsEnum.docsAndPositions(acceptDocs, null, DocsAndPositionsEnum.FLAG_PAYLOADS);
            if (postings == null)
                throw new IllegalStateException("field <" + term.field() + "> was indexed without position data");

            PerTerm pt = new PerTerm();
            pt.postings = postings;
            pt.doc_freq = termsEnum.docFreq(); // FIXME: the number here is completely wrong, it is just from the local segment. wrong, poc
            pt.per_term_collection_statistics = clj_context.collection_statistics.get(term);
            pt.token_position = pos;
            clj_context.per_term.add(pt);

            return new Scorer(weight) {
                @Override
                public int docID() { return postings.docID(); }
                @Override
                public int freq() throws IOException { return postings.freq(); }
                @Override
                public int nextDoc() throws IOException { return Helper.next_doc_and_next_position(postings); }
                @Override
                public int advance(int target) throws IOException { return Helper.advance_and_next_position(postings,target); }
                @Override
                public long cost() { return postings.cost(); }
                @Override
                public String toString() { return "scorer(" + weight.getQuery().toString() + ")"; }
                @Override
                public float score() throws IOException {
                    throw new IllegalStateException("nobody should use this score()");
                }
            };
        }
    }
}
