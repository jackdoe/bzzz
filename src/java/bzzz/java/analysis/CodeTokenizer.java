package bzzz.java.analysis;
import bzzz.java.query.*;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.payloads.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeFactory;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.symmetric.*;

public class CodeTokenizer extends Tokenizer {
    public static int MIN_TOKEN_LEN = 2;
    public static int MAX_TOKEN_LEN = 64;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);

    Iterator<Map.Entry<String,EWAHCompressedBitmap>> iterator = null;

    public CodeTokenizer(Reader input) {
        super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
        // expect call to reset(), which will fill all the tokens for the current document
        if (iterator == null)
            throw new IllegalStateException("must call reset() before incrementToken()");

        while (iterator.hasNext())
        {
            Map.Entry<String,EWAHCompressedBitmap> entry = iterator.next();

            clearAttributes();
            termAtt.setEmpty().append(entry.getKey());

            byte[] serialized = Helper.serialize_compressed_bitmap(entry.getValue());
            payAtt.setPayload(new BytesRef(serialized, 0, serialized.length));

            return true;
        }
        return false;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        Map<String,EWAHCompressedBitmap> everything = processInput(input);
        iterator = everything.entrySet().iterator();
    }

    public static void appendAndClearToken(Map<String,EWAHCompressedBitmap> into, StringBuilder token, int line) {
        if (token.length() >= MIN_TOKEN_LEN && token.length() < MAX_TOKEN_LEN) {
            String s = token.toString();

            EWAHCompressedBitmap val = into.get(s);
            if (val == null) {
                val = EWAHCompressedBitmap.bitmapOf(line);
                into.put(s,val);
            } else {
                val.set(line);
            }
        }
        token.setLength(0);
    }

    public static Map<String,EWAHCompressedBitmap> processInput(Reader input) throws IOException {
        Map<String,EWAHCompressedBitmap> result = new HashMap<String,EWAHCompressedBitmap>();
        int line = 0;
        int ch;
        int prev_symbol = -1;
        StringBuilder sb = new StringBuilder();
        while ((ch = input.read()) != -1) {
            if (ch == '\n' || ch == '\r') {
                appendAndClearToken(result,sb,line);
                line++;
                prev_symbol = -1;
            } else {
                if ((prev_symbol != -1 && prev_symbol != ch)) {
                    appendAndClearToken(result,sb,line);
                    prev_symbol = -1;
                }

                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9')) {
                    sb.append((char) ch);
                } else {
                    if (prev_symbol == -1 || (prev_symbol != -1 && prev_symbol != ch)) {
                        appendAndClearToken(result,sb,line);
                        prev_symbol = -1;
                    }
                    if (ch != ' ' && ((ch >= ':' && ch <= '@') || (ch >= '!' && ch <= '/'))) {
                        sb.append((char) ch);
                        prev_symbol = ch;
                    } else {
                        // index only symbols that occur more than once, like ==, **
                        appendAndClearToken(result,sb,line);
                        prev_symbol = -1;
                    }
                }
            }
        }
        appendAndClearToken(result,sb,line);

        return result;
    }
}
