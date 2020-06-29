package zhangjie.io.util;

import java.io.File;

/**
 * @Author zhangjie
 * @Date 2020/6/28 9:54
 **/
public class StorePathConfigHelper {
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }
}
