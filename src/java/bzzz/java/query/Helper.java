package bzzz.java.query;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import java.io.*;

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
}
