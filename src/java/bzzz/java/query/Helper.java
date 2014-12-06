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
}
