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
import java.io.StringReader;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;

public class TermPayloadClojureScoreQuery extends Query {
    public static int EXPR_CACHE_CAPACITY = 10000;
    public static Map<String,IFn> EXPR_CACHE = new Builder<String,IFn>().maximumWeightedCapacity(EXPR_CACHE_CAPACITY).build();
    public Term term;
    public String expr;
    public Map<Object,Object> local_state = new HashMap<Object,Object>();
    public String[] field_cache_req;
    public IFn clj_expr;

    public static final Var EVAL = RT.var("clojure.core", "eval");
    public static final Var READ_STRING = RT.var("clojure.core", "read-string");

    public TermPayloadClojureScoreQuery(Term term, String expr, String[] field_cache_req) throws Exception {
        this.term = term;
        this.expr = expr;
        this.field_cache_req = field_cache_req;
        this.clj_expr = EXPR_CACHE.get(expr);
        if (this.clj_expr == null) {
            this.clj_expr = (IFn) EVAL.invoke(READ_STRING.invoke(expr));
            EXPR_CACHE.put(expr,this.clj_expr);
        }
    }

    @Override
    public String toString(String field) { return term.toString() + "@" + expr; }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        final Query query = this;
        return new Weight() {
            @Override
            public String toString() { return "clojure-payload-score-weight(" + query.toString() + ")"; }
            @Override
            public Query getQuery() { return query; }
            @Override
            public float getValueForNormalization() throws IOException { return 1; }
            @Override
            public void normalize(float queryNorm, float topLevelBoost) {}
            @Override
            public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                return createScorer(context,acceptDocs,null);
            }
            @Override
            public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
                Explanation e = new Explanation();
                Scorer s = createScorer(context,context.reader().getLiveDocs(),e);
                if (s != null && s.advance(doc) == doc ) {
                    float score = s.score();
                    e.setValue(score);
                    e.setDescription("result of: " + expr);
                    return e;
                }
                return new Explanation(0f,"no matching term");
            }

            public Scorer createScorer(AtomicReaderContext context, Bits acceptDocs, final Explanation explanation) throws IOException {
                Terms terms = context.reader().terms(term.field());
                if (terms == null)
                    return null;

                final TermsEnum termsEnum = terms.iterator(null);
                if (!termsEnum.seekExact(term.bytes()))
                    return null;

                final Weight weight = this;
                final TermState state = termsEnum.termState();
                termsEnum.seekExact(term.bytes(), state);
                final DocsAndPositionsEnum postings = termsEnum.docsAndPositions(acceptDocs, null, DocsAndPositionsEnum.FLAG_PAYLOADS);
                if (postings == null)
                    throw new IllegalStateException("field <" + term.field() + "> was indexed without position data");

                final Map<String,Object> fc = new HashMap<String,Object>();
                if (field_cache_req != null) {
                    AtomicReader r = context.reader();
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
                        int payload = Helper.decode_int_payload(postings.getPayload());
                        return (float) clj_expr.invoke(explanation,payload,local_state,fc,docID());
                    }
                };
            }
        };
    }
}
