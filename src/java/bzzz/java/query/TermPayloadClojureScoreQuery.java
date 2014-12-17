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
import bzzz.java.query.ExpressionContext;

public class TermPayloadClojureScoreQuery extends Query {
    // THIS QUERY IS NOT THREAD SAFE! at the moment this is ok because of the way we create one (future) per shard
    // and the query is created in the (future) thread itself.

    public static int EXPR_CACHE_CAPACITY = 10000; // TODO: make this a parameter
    public static Map<String,IFn> EXPR_CACHE = new Builder<String,IFn>().maximumWeightedCapacity(EXPR_CACHE_CAPACITY).build();
    public static Map<Object,Object> GLOBAL_EXPR_CACHE = new Builder<Object,Object>().maximumWeightedCapacity(EXPR_CACHE_CAPACITY).build();

    // the clj_context was moved to the query from the Weight easier access for dynamic facets
    public final ExpressionContext clj_context;
    public Term term;
    public String expr;
    public String[] field_cache_req;
    public IFn clj_expr;

    public TermPayloadClojureScoreQuery(Term term, String expr, String[] field_cache_req, List<Map<Object,Object>> fba_settings) throws Exception {
        this.term = term;
        this.expr = expr;
        this.field_cache_req = field_cache_req;
        this.clj_expr = eval_and_cache(expr);
        this.clj_context = new ExpressionContext(GLOBAL_EXPR_CACHE,fba_settings);
    }

    public IFn eval_and_cache(String raw) {
        // there is of course a race condition between get/compile/put
        // but worst case few threads will do the eval, which
        // will just result in few ms extra to those calls
        IFn e = EXPR_CACHE.get(raw);
        if (e == null) {
            try {
                // since we are compiling only once, it makese sense to warn-on-reflection
                // it might be very costly for 5 million score() calls to do (.get local-state "key")
                // with reflection, when a simple hint might save you.
                Var.pushThreadBindings(RT.map(RT.var("clojure.core","*warn-on-reflection*"),RT.T));
                e = (IFn) clojure.lang.Compiler.load(new StringReader(raw));
            } finally {
                Var.popThreadBindings();
            }
            EXPR_CACHE.put(raw,e);
        }
        return e;
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
                return createScorer(context,acceptDocs);
            }
            @Override
            public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
                clj_context.explanation = null;
                Scorer s = createScorer(context,context.reader().getLiveDocs());
                if (s != null && s.advance(doc) == doc ) {
                    clj_context.explanation = new Explanation();
                    float score = s.score();
                    clj_context.explanation.setValue(score);
                    clj_context.explanation.setDescription("result of: " + expr);
                    return clj_context.explanation;
                }
                return new Explanation(0f,"no matching term");
            }

            public Scorer createScorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
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

                clj_context.postings = postings;
                clj_context.fill_field_cache(context.reader(),field_cache_req);

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
                        clj_context.freq = freq();
                        clj_context.docID = docID();
                        return (float) clj_expr.invoke(clj_context);
                    }
                };
            }
        };
    }
}
