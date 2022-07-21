package ai.sapper.hcdc.common.utils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.security.MessageDigest;

public class ChecksumUtils {
    public static String checksum(byte[] data, int length) throws IOException {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(data.length > 0);
        Preconditions.checkArgument(data.length >= length);
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(data, 0, length);
            byte[] bytes = digest.digest();

            // this array of bytes has bytes in decimal format
            // so we need to convert it into hexadecimal format

            // for this we create an object of StringBuilder
            // since it allows us to update the string i.e. its
            // mutable
            StringBuilder sb = new StringBuilder();

            // loop through the bytes array
            for (int i = 0; i < bytes.length; i++) {

                // the following line converts the decimal into
                // hexadecimal format and appends that to the
                // StringBuilder object
                sb.append(Integer
                        .toString((bytes[i] & 0xff) + 0x100, 16)
                        .substring(1));
            }

            // finally we return the complete hash
            return sb.toString();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
