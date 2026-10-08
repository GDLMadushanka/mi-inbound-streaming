import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.wso2.carbon.inbound.streaming.Utils;

import java.util.Properties;

/**
 * Times the VFS call the inbound makes before it ever opens a file - resolveFile + exists(), the
 * pair inside Utils.isFailRecord - against an SFTP endpoint that accepts and then stalls.
 *
 *   args: <uri> <timeoutMillis|none> <watchdogSeconds>
 *
 * Exit 0 = the call returned or threw within the watchdog; 1 = still blocked (the bug).
 */
public class SftpTimeoutProbe {

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        String timeout = args[1];
        long watchdogMs = Long.parseLong(args[2]) * 1000L;

        Properties params = new Properties();
        if (!"none".equalsIgnoreCase(timeout)) {
            params.setProperty("transport.vfs.SFTPTimeout", timeout);
        }
        // Keep the connect bounded only by what we are measuring.
        params.setProperty("transport.vfs.SFTPConnectTimeout", "none".equalsIgnoreCase(timeout)
                ? "600000" : timeout);

        FileSystemManager fsManager = VFS.getManager();
        FileSystemOptions fso =
                Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(uri, params), fsManager);

        System.out.println("  SFTPTimeout = " + ("none".equalsIgnoreCase(timeout) ? "<unset>" : timeout + "ms"));
        final long start = System.currentTimeMillis();
        final Throwable[] thrown = new Throwable[1];
        final boolean[] done = new boolean[1];

        Thread call = new Thread(() -> {
            try {
                FileObject fo = fsManager.resolveFile(uri, fso);
                fo.exists();
            } catch (Throwable t) {
                thrown[0] = t;
            } finally {
                done[0] = true;
            }
        }, "vfs-call");
        call.setDaemon(true);
        call.start();
        call.join(watchdogMs);

        long elapsed = System.currentTimeMillis() - start;
        if (!done[0]) {
            System.out.println("  STILL BLOCKED after " + elapsed + "ms  <-- the poller thread is stuck");
            System.exit(1);
        }
        String how = thrown[0] == null ? "returned normally"
                : "threw " + rootCause(thrown[0]).getClass().getSimpleName()
                  + ": " + String.valueOf(rootCause(thrown[0]).getMessage()).split("\n")[0];
        System.out.println("  unblocked after " + elapsed + "ms (" + how + ")");
    }

    private static Throwable rootCause(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t;
    }
}
