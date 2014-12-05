package bzzz.java.query;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import java.io.*;

public class NoNormQuery extends Query {
    public Query query;

    public NoNormQuery(Query query) {
        this.query = query;
    }

    @Override
    public String toString(String field) { return "no-norm:" + query.toString(field); }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        final Weight weight = query.createWeight(searcher);
        return new Weight() {
            @Override
            public String toString() { return "no-norm:" + weight.toString(); }

            @Override
            public Query getQuery() { return weight.getQuery(); }

            @Override
            public float getValueForNormalization() throws IOException { return weight.getValueForNormalization(); }

            @Override
            public void normalize(float queryNorm, float topLevelBoost) { weight.normalize(1,topLevelBoost); };

            @Override
            public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
                return weight.explain(context,doc);
            }

            @Override
            public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                final Scorer scorer = weight.scorer(context,acceptDocs);
                if (scorer == null)
                    return scorer;
                return new Scorer(weight) {
                    @Override
                    public int docID() { return scorer.docID(); }

                    @Override
                    public int freq() throws IOException { return scorer.freq(); }

                    @Override
                    public int nextDoc() throws IOException { return scorer.nextDoc(); }

                    @Override
                    public int advance(int target) throws IOException { return scorer.advance(target); };

                    @Override
                    public float score() throws IOException { return scorer.score(); }

                    @Override
                    public long cost() { return scorer.cost(); }

                    @Override
                    public String toString() { return "no-norm:" + scorer.toString(); }
                };
            }
        };
    }
}
