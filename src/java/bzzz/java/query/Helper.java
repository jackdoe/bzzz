package bzzz.java.query;

import bzzz.java.query.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import java.io.*;
import java.util.*;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.symmetric.*;
public class Helper {
    public static int decode_int_payload(BytesRef p) {
        if (p == null)
            return 0;
        return PayloadHelper.decodeInt(p.bytes,p.offset);
    }

    public static byte[] serialize_compressed_bitmap(EWAHCompressedBitmap bitmap) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bos);
            bitmap.writeExternal(oo);
            oo.close();
            return bos.toByteArray();
    }

    public static int next_doc_and_next_position(DocsAndPositionsEnum postings) throws IOException {
        int n = postings.nextDoc();
        if (n != DocsAndPositionsEnum.NO_MORE_DOCS)
            postings.nextPosition();
        return n;
    }

    public static int advance_and_next_position(DocsAndPositionsEnum postings, int target) throws IOException {
        int n = postings.advance(target);
        if (n != DocsAndPositionsEnum.NO_MORE_DOCS)
            postings.nextPosition();
        return n;
    }

    public static List<Query> collect_possible_subqueries(Query top, List<Query> result, Class filter) {
        if (result == null)
            result = new ArrayList<Query>();
        if (top instanceof BooleanQuery) {
            for (BooleanClause bq : ((BooleanQuery)top).clauses())
                collect_possible_subqueries(bq.getQuery(), result, filter);
        } else if (top instanceof DisjunctionMaxQuery) {
            for (Query q : ((DisjunctionMaxQuery)top).getDisjuncts())
                collect_possible_subqueries(q, result,filter);
        } else if (top instanceof ConstantScoreQuery) {
            collect_possible_subqueries(((ConstantScoreQuery)top).getQuery(), result,filter);
        } else if (top instanceof NoZeroQuery) {
            collect_possible_subqueries(((NoZeroQuery)top).query, result,filter);
        } else if (top instanceof NoNormQuery) {
            collect_possible_subqueries(((NoNormQuery)top).query, result,filter);
        } else {
            if (filter == null || top.getClass() == filter)
                result.add(top);
        }
        return result;
    }

    public static float object_to_float(Object s) {
        if (s instanceof Integer)
            return ((Integer) s).floatValue();
        else if (s instanceof Long)
            return ((Long) s).floatValue();
        else
            return (Float) s;
    }

    public static List<String> tokenize(String fieldName, String text, Analyzer analyzer) throws IOException {
        List<String> out = new ArrayList<String>();

        TokenStream stream = analyzer.tokenStream(fieldName, new StringReader(text));
        stream.reset();
        while (stream.incrementToken()) {
            out.add(stream.getAttribute(CharTermAttribute.class).toString());
        }
        stream.end();
        stream.close();

        return out;
    }

    public static List<TermPayload> tokenize_into_term_payload(String fieldName, String text, Analyzer analyzer) throws IOException {
        List<TermPayload> out = new ArrayList<TermPayload>();

        TokenStream stream = analyzer.tokenStream(fieldName, new StringReader(text));
        stream.reset();
        while (stream.incrementToken()) {
            TermPayload tp = new TermPayload();
            tp.term = stream.getAttribute(CharTermAttribute.class).toString();
            tp.payload = BytesRef.deepCopyOf(stream.getAttribute(PayloadAttribute.class).getPayload());
            out.add(tp);
        }
        stream.end();
        stream.close();

        return out;
    }

    public static final class TermPayload {
        public String term;
        public BytesRef payload;
        public String toString() {
            return term + " - " + payload.toString();
        }
    }
}
