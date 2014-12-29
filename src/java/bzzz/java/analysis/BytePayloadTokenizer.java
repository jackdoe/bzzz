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

public class BytePayloadTokenizer extends Tokenizer {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);

    public final StringBuilder sb = new StringBuilder();
    public final byte[] buffer = new byte[4096];

    public BytePayloadTokenizer(Reader input) {
        super(input);
    }

    public static int hex_char_to_int(int c) {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return (c - 'a') + 10;
        if (c >= 'A' && c <= 'F')
            return (c - 'A') + 10;
        throw new IllegalStateException("character out of range");
    }

    @Override
    public void reset() throws IOException {
        super.reset();
    }

    @Override
    public boolean incrementToken() throws IOException {
        sb.setLength(0);
        int ch;
        boolean in_string = true;
        int nibble = 0;

        while ((ch = input.read()) != -1) {
            if (ch == ' ') {
                if (sb.length() > 0 && nibble > 0) {
                    clearAttributes();
                    termAtt.setEmpty().append(sb.toString());
                    byte[] copy = new byte[nibble/2];
                    System.arraycopy(buffer,0,copy,0,copy.length);
                    payAtt.setPayload(new BytesRef(copy));
                    return true;
                }
                nibble = 0;
                in_string = true;
            } else {
                if (ch == '|') {
                    in_string = false;
                    nibble = 0;
                } else {
                    if (in_string) {
                        sb.append((char) ch);
                    } else {
                        int val = hex_char_to_int(ch);
                        // ABDE
                        // 0123
                        // for every even nibble, we overwrite the buffer
                        // every odd we | it
                        if ((nibble & 1) == 0)
                            buffer[nibble/2] = (byte) (val << 4);
                        else
                            buffer[nibble/2] |= (byte) val;

                        nibble++;
                    }
                }
            }
        }

        if (sb.length() > 0 && nibble > 0) {
            clearAttributes();
            termAtt.setEmpty().append(sb.toString());
            byte[] copy = new byte[nibble/2];
            System.arraycopy(buffer,0,copy,0,copy.length);
            payAtt.setPayload(new BytesRef(copy));
            return true;
        }
        return false;
    }
}
