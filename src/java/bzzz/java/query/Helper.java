package bzzz.java.query;
import bzzz.java.query.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import java.io.*;
import java.util.*;
public class Helper {
    public static int decode_int_payload(BytesRef p) {
        if (p == null)
            return 0;
        return PayloadHelper.decodeInt(p.bytes,p.offset);
    }

    public static int next_doc_and_next_position(DocsAndPositionsEnum postings) throws IOException {
        int n = postings.nextDoc();
        if (n != DocsAndPositionsEnum.NO_MORE_DOCS)
            postings.nextPosition();
        return n;
    }
    public static int advance_and_next_position(DocsAndPositionsEnum postings, int target) throws IOException {
        int n = postings.advance(target);
        // XXX: in case n != target, maybe we should not do nextPosition()
        //      it will be ignored anyway
        if (n != DocsAndPositionsEnum.NO_MORE_DOCS)
            postings.nextPosition();
        return n;
    }

    public static List<Query> collect_possible_subqueries(Query top, List<Query> result) {
        if (result == null)
            result = new ArrayList<Query>();
        if (top instanceof BooleanQuery) {
            for (BooleanClause bq : ((BooleanQuery)top).clauses())
                collect_possible_subqueries(bq.getQuery(), result);
        } else if (top instanceof DisjunctionMaxQuery) {
            for (Query q : ((DisjunctionMaxQuery)top).getDisjuncts())
                collect_possible_subqueries(q, result);
        } else if (top instanceof ConstantScoreQuery) {
            collect_possible_subqueries(((ConstantScoreQuery)top).getQuery(), result);
        } else if (top instanceof NoZeroQuery) {
            collect_possible_subqueries(((NoZeroQuery)top).query, result);
        } else if (top instanceof NoNormQuery) {
            collect_possible_subqueries(((NoNormQuery)top).query, result);
        } else {
            result.add(top);
        }
        return result;
    }
}
