package funv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by kingdee on 2017/1/12.
 */
public class hadoopbasic {
    /**
     * 文件检测并删除
     *
     * @param path
     * @param conf
     * @return
     */
    public static boolean checkAndDel(final String path, Configuration conf) {
        Path dstPath = new Path(path);
        try {
            FileSystem dhfs = dstPath.getFileSystem(conf);
            if (dhfs.exists(dstPath)) {
                return true;
            } else {
                return false;
            }
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }

    }
}
