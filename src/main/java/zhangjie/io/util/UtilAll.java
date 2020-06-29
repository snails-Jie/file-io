package zhangjie.io.util;

import java.text.NumberFormat;

/**
 * @Author zhangjie
 * @Date 2020/6/27 11:16
 **/
public class UtilAll {
    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }
}
