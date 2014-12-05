package bzzz.java.query;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import java.io.*;

public class NoZeroQuery extends Query {
    public Query query;

    public NoZeroQuery(Query query) {
        this.query = query;
    }

    @Override
    public String toString(String field) { return "no-zero:" + query.toString(field); }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        final Weight weight = query.createWeight(searcher);
        return new Weight() {
            @Override
            public String toString() { return "no-zero:" + weight.toString(); }

            @Override
            public Query getQuery() { return weight.getQuery(); }

            @Override
            public float getValueForNormalization() throws IOException { return weight.getValueForNormalization(); }

            @Override
            public void normalize(float queryNorm, float topLevelBoost) { weight.normalize(queryNorm,topLevelBoost); }

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
                    float current_score = -1f;
                    @Override
                    public int docID() { return scorer.docID(); }

                    @Override
                    public int freq() throws IOException { return scorer.freq(); }

                    @Override
                    public int nextDoc() throws IOException {
                        while(true) {
                            int n = scorer.nextDoc();
                            if (n == DocIdSetIterator.NO_MORE_DOCS)
                                return n;
                            current_score = scorer.score();
                            if (current_score != 0)
                                return n;
                        }
                    }
                    @Override
                    public int advance(int target) throws IOException {
                        int n = scorer.advance(target);
                        if (n == DocIdSetIterator.NO_MORE_DOCS)
                            return n;
                        current_score = scorer.score();
                        if (current_score != 0)
                            return n;
                        // if the score is 0, we just return the next non-zero next doc
                        return nextDoc();
                    }

                    @Override
                    public float score() throws IOException { return current_score; }

                    @Override
                    public long cost() { return scorer.cost(); }

                    @Override
                    public String toString() { return "no-zero:" + scorer.toString(); }

                };
            }
        };
    }
}
