package bzzz.java.analysis;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.lang.StringBuilder;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.payloads.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.ArrayUtil;

public class CodeTokenizer extends Tokenizer {
    public static int FLAG_IMPORTANT = 1 << 31;
    public static final Set<String> IMPORTANT = new HashSet<String>(Arrays.asList("sub","package","public","private"));

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);
    private final StringBuilder sb = new StringBuilder();

    int line = 0;
    int flags = 0;
    int line_offset = 0;
    int line_flags = 0;
    public CodeTokenizer(Reader input,int line_offset, int flags) {
        super(input);
        this.flags = flags;
        this.line_offset = line_offset;
    }

    public boolean emmit() {
        if (sb.length() > 0) {
            clearAttributes();
            byte[] bytes = PayloadHelper.encodeInt((line + line_offset) | flags | line_flags);
            payAtt.setPayload(new BytesRef(bytes));

            termAtt.setEmpty().append(sb);
            if (IMPORTANT.contains(sb.toString()))
                line_flags |= FLAG_IMPORTANT;

            sb.setLength(0);
            return true;
        }
        return false;
    }

    @Override
    public boolean incrementToken() throws IOException {
        int ch, prev_symbol = -1;
        while ((ch = input.read()) != -1) {
            if (ch == '\n' || ch == '\r') {
                boolean emmitted = emmit();
                line++;

                if (emmitted)
                    return true;

                line_flags = 0;

                prev_symbol = -1;
            } else {
                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
                    sb.append((char) ch);
                    prev_symbol = -1;
                } else {
                    if (prev_symbol == -1 || prev_symbol != ch)
                        if (emmit())
                            return true;

                    if (ch != ' ' && ch != ';' && ((ch >= ':' && ch < '@') || (ch >= '!' && ch <= '/'))) {
                        if (prev_symbol == -1 || ch == prev_symbol) {
                            sb.append((char) ch);
                            prev_symbol = ch;
                        } else {
                            if(emmit())
                                return true;
                            prev_symbol = -1;
                        }

                    }
                }
            }
        }
        return false;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        line = 0;
        sb.setLength(0);
    }
}
