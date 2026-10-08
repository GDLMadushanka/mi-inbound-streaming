import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.wso2.carbon.inbound.streaming.Utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * Times one VFS operation against an SFTP endpoint, with or without transport.vfs.SFTPTimeout,
 * under a watchdog.
 *
 *   args: <uri> <timeoutMillis|none> <watchdogSeconds> <stat|list|read>
 *
 * list maps onto ChannelSftp.ls - the call named in the timeout report - and stat onto the
 * exists() inside Utils.isFailRecord.
 *
 * Exit 0 = the call completed or failed within the watchdog; 1 = still blocked; 2 = bad usage.
 */
public class SftpOpProbe {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: SftpOpProbe <uri> <timeoutMs|none> <watchdogSec> <stat|list|read>");
            System.exit(2);
        }
        final String uri = args[0];
        String timeout = args[1];
        long watchdogMs = Long.parseLong(args[2]) * 1000L;
        final String op = args[3];

        Properties params = new Properties();
        if (!"none".equalsIgnoreCase(timeout)) {
            params.setProperty("transport.vfs.SFTPTimeout", timeout);
            params.setProperty("transport.vfs.SFTPConnectTimeout", timeout);
        } else {
            params.setProperty("transport.vfs.SFTPConnectTimeout", "600000");
        }

        final FileSystemManager fsManager = VFS.getManager();
        final FileSystemOptions fso =
                Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(uri, params), fsManager);

        System.out.println("  op=" + op + "  SFTPTimeout="
                + ("none".equalsIgnoreCase(timeout) ? "<unset>" : timeout + "ms"));

        final long start = System.currentTimeMillis();
        final Throwable[] thrown = new Throwable[1];
        final String[] detail = new String[1];
        final boolean[] done = new boolean[1];

        Thread call = new Thread(() -> {
            try {
                FileObject fo = fsManager.resolveFile(uri, fso);
                if ("stat".equals(op)) {
                    detail[0] = "exists=" + fo.exists();
                } else if ("list".equals(op)) {
                    FileObject[] kids = fo.getChildren();
                    StringBuilder sb = new StringBuilder(kids.length + " entries:");
                    for (FileObject k : kids) {
                        sb.append(' ').append(k.getName().getBaseName());
                    }
                    detail[0] = sb.toString();
                } else {
                    int n = 0;
                    try (InputStream in = fo.getContent().getInputStream()) {
                        byte[] buf = new byte[8192];
                        int r;
                        while ((r = in.read(buf)) != -1) {
                            n += r;
                        }
                    }
                    detail[0] = n + " bytes read";
                }
            } catch (Throwable t) {
                thrown[0] = t;
            } finally {
                done[0] = true;
            }
        }, "vfs-op");
        call.setDaemon(true);
        call.start();
        call.join(watchdogMs);

        long elapsed = System.currentTimeMillis() - start;
        if (!done[0]) {
            System.out.println("  STILL BLOCKED after " + elapsed + "ms  <-- the poller thread is stuck");
            System.exit(1);
        }
        if (thrown[0] != null) {
            Throwable c = thrown[0];
            while (c.getCause() != null && c.getCause() != c) {
                c = c.getCause();
            }
            System.out.println("  failed after " + elapsed + "ms: " + c.getClass().getSimpleName()
                    + ": " + String.valueOf(c.getMessage()).split("\n")[0]);
        } else {
            System.out.println("  OK in " + elapsed + "ms - " + detail[0]);
        }
    }
}
