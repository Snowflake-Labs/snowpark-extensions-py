import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * This class is a patched version of {@link RawLocalFileSystem} to allow using Hadoop methods inside
 * of the Snowpark Sandbox
 */
public class RawLocalFileSystemPatched extends RawLocalFileSystem {

      public RawLocalFileSystemPatched() {
        super();
      }
      @Override
      public void setPermission(Path p, FsPermission permission) throws IOException { 
        // do nothing
      }
}