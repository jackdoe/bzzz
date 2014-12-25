package bzzz.java.analysis;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.*;
import org.apache.lucene.analysis.payloads.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.AttributeFactory;

public class CodeTokenizer extends Tokenizer {
    public static int FLAG_IMPORTANT = 1 << 31;
    public static int AGAIN = -2;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);
    private final CodeToken current_token = new CodeToken();

    int line = 0;
    int flags = 0;
    int line_offset = 0;
    int line_flags = 0;
    int ch = 0;
    int reuse = 0;

    public CodeTokenizer(Reader input,int line_offset, int flags) {
        super(input);
        this.flags = flags;
        this.line_offset = line_offset;
    }

    public boolean emmit(int ov) {
        reuse = ov;
        if (current_token.length > 0) {
            clearAttributes();
            current_token.copy_into_attributes(termAtt,payAtt,(line + line_offset) | flags | line_flags);

            if (current_token.is_important())
                line_flags |= FLAG_IMPORTANT;

            current_token.reset();

            return true;
        }
        return false;
    }

    @Override
    public boolean incrementToken() throws IOException {
        int prev_symbol = -1;

        while ((reuse == AGAIN) || (ch = input.read()) != -1) {
            if (ch == '\n' || ch == '\r') {
                boolean emmitted = emmit(0);
                line++;
                if (emmitted) return true;

                line_flags = 0;

                prev_symbol = -1;
            } else {

                if ((prev_symbol != -1 && prev_symbol != ch)) {
                    if (emmit(AGAIN)) return true;
                    prev_symbol = -1;
                }

                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9')) {
                    current_token.append(ch);
                } else {
                    if (prev_symbol == -1 || (prev_symbol != -1 && prev_symbol != ch)) {
                        if (emmit(AGAIN)) return true;
                        prev_symbol = -1;
                    }

                    if (ch != '.' && ch != ' ' && ch != ';' && ((ch >= ':' && ch <= '@') || (ch >= '!' && ch <= '/'))) {
                        current_token.append(ch);
                        prev_symbol = ch;
                    } else {
                        if (emmit(0)) return true;
                        prev_symbol = -1;
                    }
                }
            }
            reuse = 0;
        }

        if (emmit(0)) return true;

        return false;
    }

    // keywords:
    // int main void() {
    // will emmit int, main, void, void( ()
    // a = b, should emit a,b,a= =b
    // void ***, void void***

    @Override
    public void reset() throws IOException {
        super.reset();
        line = 0;
        line_flags = 0;
        ch = 0;
        current_token.reset();
    }

    public static final class CodeToken {
        public static final char[][] IMPORTANT = {{'s','u','b'},
                                                  {'p','a','c','k','a','g','e'},
                                                  {'p','u','b','l','i','c','p','r','i','v','a','t','e'}};
        public static int min_important_token_len = 3;

        public static int MAX_TOKEN_LEN = 256;
        public char[] buffer = new char[MAX_TOKEN_LEN];
        public byte[] payload_buffer = new byte[4];

        public int length = 0;

        public void append(int c) {
            if (this.length < buffer.length) {
                buffer[this.length] = (char) c;
                this.length++;
            }
        }

        public int fit_and_inc_len(int len) {
            int to_write = Math.min(buffer.length - this.length, len);
            this.length += to_write;
            return to_write;
        }

        public void append(char[] b) {
            int write_pos = length;
            System.arraycopy(b, 0, buffer, write_pos, fit_and_inc_len(b.length));
        }

        public void append(String s) {
            int write_pos = this.length;
            s.getChars(0,fit_and_inc_len(s.length()),buffer, write_pos);
        }

        public void append(StringBuilder sb) {
            int write_pos = this.length;
            sb.getChars(0,fit_and_inc_len(sb.length()),buffer, write_pos);
        }

        public void reset() {
            length = 0;
        }

        public void encodeInt(int payload){
            payload_buffer[0] = (byte)(payload >> 24);
            payload_buffer[1] = (byte)(payload >> 16);
            payload_buffer[2] = (byte)(payload >>  8);
            payload_buffer[3] = (byte) payload;
        }
        public void copy_into_attributes(CharTermAttribute catt, PayloadAttribute patt,int payload) {
            encodeInt(payload);
            patt.setPayload(new BytesRef(payload_buffer));
            catt.resizeBuffer(length);
            catt.setLength(length);
            catt.copyBuffer(buffer,0,this.length);
        }

        public boolean is_important() {
            if (this.length < min_important_token_len)
                return false;

            for (int i = 0; i < IMPORTANT.length; i++) {
                if (IMPORTANT[i].length == this.length) {
                    boolean mismatch = false;
                    for (int j = 0; j < IMPORTANT[i].length; j++) {
                        if (IMPORTANT[i][j] != buffer[j]) {
                            mismatch = true;
                            break;
                        }
                    }
                    if (!mismatch)
                        return true;
                }
            }
            return false;
        }
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < buffer.length; i++) {

                if (i < this.length) {
                    sb.append(buffer[i]);
                } else if (buffer[i] != 0) {
                    sb.append("[");
                    sb.append(buffer[i]);
                    sb.append("]");
                }
            }
            return sb.toString();
        }
    }
}
