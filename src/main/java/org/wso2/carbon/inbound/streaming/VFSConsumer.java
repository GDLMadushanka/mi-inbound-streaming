/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.streaming;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.streaming.filter.FileSelector;
import org.wso2.carbon.inbound.streaming.lock.LockManager;
import org.wso2.carbon.inbound.streaming.processor.MoveAction;
import org.wso2.carbon.inbound.streaming.processor.PostProcessingHandler;
import org.wso2.carbon.inbound.streaming.processor.PreProcessingHandler;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.cache.NullFilesCache;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.inbound.streaming.Utils.maskURLPassword;
import static org.wso2.carbon.inbound.streaming.Utils.stripVfsSchemeIfPresent;

public class VFSConsumer extends GenericPollingConsumer {

    private static final Log log = LogFactory.getLog(VFSConsumer.class);
    private static final String EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e";
    private final VFSConfig vfsConfig;
    private final FileSelector fileSelector;
    private final PreProcessingHandler preProcessingHandler;
    private final PostProcessingHandler postProcessingHandler;
    private final FileInjectHandler fileInjectHandler;
    private final String name;
    private FileSystemManager fsManager = null;
    private ScheduledExecutorService retryScheduler;
    private boolean fileLock = true;
    private FileSystemOptions fso;
    private boolean readSubDirectories = false;
    // Read from the poll thread and written from the shutdown thread, so it must be volatile.
    private volatile boolean isClosed;
    // True only while a STREAMING file is being processed. close() waits for this to clear before
    // closing the file system manager, so the in-flight stream can finish - releasing its lock -
    // instead of having the file system torn out from under it. The
    // non-streaming path never sets this, so its shutdown behaviour is unchanged.
    private volatile boolean processing;
    // Maximum time close() waits for the in-flight poll to finish before forcing the shutdown.
    // The JVM shutdown hook below normally cancels the stream well before close() is reached, so
    // this is a backstop for the paths that call destroy() directly (undeploy, task pause).
    private static final long SHUTDOWN_WAIT_MILLIS = 10000L;
    private static final long SHUTDOWN_POLL_MILLIS = 100L;
    private String replyFileURI;
    private String replyFileName;
    private boolean append = false;
    private boolean resolveHostsDynamically = false;
    private Long failedRecordNextRetryDuration = 30000L; // Default 30 seconds
    private boolean isMounted = false;

    private String fileURI;
    // Cancels the in-flight stream the moment the JVM is asked to exit. Carbon only destroys the
    // inbound after Quartz has waited for the running poll to finish, which on a multi-GB file is
    // minutes - by then the shutdown has already stalled. SIGTERM (what bin/micro-integrator.sh
    // stop sends) runs this hook straight away instead.
    private Thread shutdownHook;

    public VFSConsumer(Properties properties,
                       String name,
                       SynapseEnvironment synapseEnvironment,
                       long scanInterval,
                       String injectingSeq,
                       String onErrorSeq,
                       boolean coordination,
                       boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);

        this.vfsConfig = new VFSConfig(properties);

        try {
            StandardFileSystemManager mgr = new StandardFileSystemManager();
            mgr.setClassLoader(getClass().getClassLoader());
            // we don't need to cache files in the consumer side
            mgr.setFilesCache(new NullFilesCache());
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error initializing VFS FileSystemManager", e);
        }

        // Build FSO once per poll (protocol options; auth; SFTP opts; etc.)
        this.fso = null;

        // Initialize new features
        initializeProperties();

        // Initialize retry scheduler for failed records
        this.retryScheduler = Executors.newScheduledThreadPool(1);

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment,
                vfsConfig, fsManager);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess, vfsConfig.getMoveAfterProcess(), fsManager));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure(), vfsConfig.getMoveAfterFailure(), fsManager));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;

        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            VFSTransportErrorHandler.handleException(log, "Invalid FileURI. Check configuration. URI: " + maskURLPassword(vfsConfig.getFileURI()));
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        // Resolve hosts dynamically if enabled
        if (resolveHostsDynamically) {
            fileURI = resolveHostDynamically(fileURI);
        }
        //Setup SFTP Options
        try {
            fso = Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(fileURI, properties), fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the sftp Options", e);
            fso = null;
        }

        registerShutdownHook();
    }

    /**
     * Ask the in-flight stream to stop as soon as the JVM starts shutting down.
     * <p>
     * Without this the server hangs: Carbon's shutdown blocks in Quartz ("Waiting for tasks to
     * finish...") until the poll returns, and the poll is streaming the whole file - nothing has
     * told it to stop, because {@code destroy()} is only called after that wait. The hook sets the
     * cancel flag instead, so the stream breaks at the next record/chunk boundary, writes its
     * checkpoint and returns, releasing the Quartz worker.
     * <p>
     * The hook only sets flags; it never blocks and never touches the file system, so it cannot
     * itself delay the exit.
     */
    private void registerShutdownHook() {
        if (!isRecordOrChunkStreaming()) {
            return;   // whole-file mode has no mid-file state to protect
        }
        shutdownHook = new Thread(() -> {
            isClosed = true;
            vfsConfig.setCanceled(true);
            log.info("JVM shutdown requested; asking the in-flight stream of inbound '" + name
                    + "' to stop at the next record boundary and checkpoint.");
        }, "vfs-streaming-shutdown-" + name);
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (IllegalStateException alreadyShuttingDown) {
            shutdownHook = null;
        }
    }

    /** Drop the hook when the inbound is undeployed, so redeploys do not accumulate hooks. */
    private void unregisterShutdownHook() {
        if (shutdownHook == null) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException shutdownInProgress) {
            // The hook is running or about to; nothing to remove.
        }
        shutdownHook = null;
    }

    // NOTE: the cron-expression constructor of the upstream inbound is not ported to MI 4.1.0.
    // GenericPollingConsumer in MI 4.1.0 only exposes the fixed-interval constructor
    // (scanInterval as a long), and GenericProcessor never passes a cron expression, so a cron
    // based poll cannot be scheduled on this runtime. Only 'interval' is supported here.

    /**
     * Initialize new VFS features
     */
    private void initializeProperties() {

        // Reply file configuration
        this.replyFileURI = vfsConfig.getReplyFileURI();
        this.replyFileName = vfsConfig.getReplayFileName();

        // Resolve hosts dynamically
        this.resolveHostsDynamically = vfsConfig.isResolveHostsDynamically();

        // Failed record retry duration
        this.failedRecordNextRetryDuration = vfsConfig.getFailedRecordNextRetryDuration();

        if (log.isDebugEnabled()) {
            log.debug("VFS Consumer initialized with features");
        }
    }

    @Override
    public Object poll() {
        if (isClosed) {
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Polling VFS location: " + maskURLPassword(fileURI) +
                    " (recursive=" + readSubDirectories + ")");
        }

        FileObject root = initFileCheck(fileURI);

        if (root == null) {
            log.error("Resolved FileObject is null for: " + maskURLPassword(fileURI));
            return null;
        }

        try {
            if (root.getType() == FileType.FILE) {
                // Single-file mode
                processFile(root);
            } else if (root.getType() == FileType.FOLDER) {
                // Directory mode (optional recursion)
                processDirectory(root);
            } else {
                // TODO: to handle FileType FILE_OR_FOLDER;
                if (log.isDebugEnabled()) {
                    log.debug("Ignoring non-file, non-folder: " + maskURLPassword(root.toString()));
                }
            }
        } catch (FileSystemException e) {
            log.error("Error during poll for: " + maskURLPassword(fileURI), e);
        } finally {
            safeClose(root);
        }

        return null;
    }

    /* =========================
       Directory & File Handling
       ========================= */

    private void processDirectory(FileObject dir) throws FileSystemException {
        if (isClosed) {
            return;
        }
        FileObject[] children = null;
        int processCount = 0;
        try {
            children = dir.getChildren();
        } catch (FileSystemException e) {
            log.error("Unable to list children of: " + maskURLPassword(dir.toString()), e);
        }

        if (children == null || children.length == 0) {
            if (log.isDebugEnabled()) {
                log.debug("Empty directory: " + maskURLPassword(dir.toString()));
            }
            return;
        }
        // 1. sort to ensure files are processed in order
        String fileSortParam = vfsConfig.getFileSortParam();
        if (StringUtils.isNotEmpty(fileSortParam)) {
            children = Utils.sortFileObjects(children, fileSortParam, vfsConfig);
        }
        for (FileObject child : children) {
            if (isClosed) {
                return;
            }
            try {
                // Skip lock/fail markers
                String base = child.getName().getBaseName();
                if (base.endsWith(".lock") || base.endsWith(".fail")) {
                    continue;
                }

                // Check if this is a failed record
                boolean isFailedRecord = Utils.isFailRecord(fsManager, child, fso) || Utils.isFailedRecordInFailedList(child, vfsConfig);
                if (isFailedRecord) {
                    // Handle failed record with retry mechanism
                    scheduleFailedRecordRetry(child);
                    continue;
                }

                if (child.getType() == FileType.FOLDER) {
                    if (readSubDirectories) {
                        // Recurse only if enabled
                        processDirectory(child);
                    } else if (log.isDebugEnabled()) {
                        log.debug("Skipping subdirectory (recursive disabled): " +
                                maskURLPassword(child.toString()));
                    }
                } else if (child.getType() == FileType.FILE) {
                    processCount++;
                    if (vfsConfig.getFileProcessingInterval() != null && vfsConfig.getFileProcessingInterval() != 0) {
                        // Throttle file processing if configured
                        try {
                            processFile(child);
                            Thread.sleep(vfsConfig.getFileProcessingInterval());
                        } catch (InterruptedException ignore) {
                            if (log.isDebugEnabled()) {
                                log.debug("File processing sleep interrupted");
                            }
                            Thread.currentThread().interrupt();
                        }
                    } else if (vfsConfig.getFileProcessingCount() != null && processCount <= vfsConfig.getFileProcessingCount()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Processing file (count limit " + vfsConfig.getFileProcessingCount() +
                                    "): " + maskURLPassword(child.toString()));
                        }
                        processFile(child);
                    } else if (vfsConfig.getFileProcessingCount() != null && processCount > vfsConfig.getFileProcessingCount()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping file (count limit " + vfsConfig.getFileProcessingCount() +
                                    " reached): " + maskURLPassword(child.toString()));
                        }
                        break;
                    } else {
                        processFile(child);
                    }

                } else if (log.isDebugEnabled()) {
                    log.debug("Ignoring item (not file/folder): " +
                            maskURLPassword(child.toString()));
                }
            } catch (Exception e) {
                // never block remaining files on a single failure
                log.error("Error processing child: " + maskURLPassword(child.toString()), e);
            } finally {
                safeClose(child);
            }
        }
    }

    private void processFile(FileObject file) throws FileSystemException {
        if (isClosed) {
            return;
        }
        file.setIsMounted(vfsConfig.isFileLocking());
        // Delegate readiness, age, size, pattern, etc. to FileSelector
        if (!fileSelector.isValidFile(file)) {
            if (log.isDebugEnabled()) {
                log.debug("File not eligible: " + maskURLPassword(file.toString()));
            }
            return;
        }

        // Acquire lock if file locking is enabled
        fileLock = vfsConfig.isFileLocking();
        LockManager lockManager = new LockManager(fileLock, vfsConfig,
                fsManager, fso);

        if (fileLock && !lockManager.acquireLock(file)) {
            log.error("Couldn't get the lock for processing the file: " +
                    maskURLPassword(file.getName().toString()));
            return;
        }

        // Only streaming (CHUNK/RECORD) stops mid-file on shutdown, so only there do we leave a
        // shutdown-interrupted file in place instead of post-processing it. Entire-file /
        // non-streaming processing keeps its normal shutdown behaviour.
        boolean recordOrChunkStreaming = isRecordOrChunkStreaming();
        // Streaming only: mark this poll busy so close() waits for it to stop (releasing the lock)
        // before closing the file system manager. No effect on the non-streaming path.
        if (recordOrChunkStreaming) {
            processing = true;
        }

        try {
            // Pre-processing hook (e.g., acquire lock, tmp rename, etc.)
            preProcessingHandler.handle(file);

            // Build transport headers
            try (FileContent content = file.getContent()) {
                String fileName = file.getName().getBaseName();
                String filePath = file.getName().getPath();
                String fileURI = file.getName().getURI();

                Map<String, Object> headers = new HashMap<>();
                headers.put(VFSConstants.FILE_NAME, fileName);
                headers.put(VFSConstants.FILE_PATH, filePath);
                headers.put(VFSConstants.FILE_URI, fileURI);

                // Add reply file information if configured
                if (StringUtils.isNotEmpty(replyFileURI)) {
                    headers.put(VFSConstants.REPLY_FILE_URI, replyFileURI);
                }
                if (StringUtils.isNotEmpty(replyFileName)) {
                    headers.put(VFSConstants.REPLY_FILE_NAME, replyFileName);
                }

                // Add append mode flag
                headers.put(VFSConstants.APPEND, String.valueOf(append));

                try {
                    headers.put(org.apache.synapse.commons.vfs.VFSConstants.FILE_LENGTH, content.getSize());
                    headers.put(org.apache.synapse.commons.vfs.VFSConstants.LAST_MODIFIED, content.getLastModifiedTime());
                } catch (FileSystemException ignore) {
                    // length/mtime are best-effort
                }

                fileInjectHandler.setTransportHeaders(headers);
                fileInjectHandler.setFileURI(fileURI);

                boolean ok = fileInjectHandler.invoke(file, name);
                if (recordOrChunkStreaming && (isClosed || vfsConfig.isCanceled())) {
                    // Server is shutting down. A streamed file stops at a record/chunk boundary;
                    // leave it where it is (no success/fail post-processing) so it is re-polled
                    // after the restart. There is no resume point, so it is streamed again from
                    // the start and its already-injected records are redelivered.
                    log.info("Server shutdown during streaming; leaving file in place to be "
                            + "reprocessed from the start on restart: "
                            + maskURLPassword(file.toString()));
                } else if (ok) {
                    if (log.isDebugEnabled()) {
                        log.debug("File processed successfully: " + maskURLPassword(file.toString()));
                    }
                    postProcessingHandler.onSuccess(file);
                } else {
                    // handle the failed records here too
                    if (log.isDebugEnabled()) {
                        log.debug("File processing failed: " + maskURLPassword(file.toString()));
                    }
                    postProcessingHandler.onFail(file);
                }
            }
        } catch (Exception e) {
            if (recordOrChunkStreaming && (isClosed || vfsConfig.isCanceled())) {
                // Shutdown race (e.g. the file system manager closed mid-read). Not a real failure -
                // leave the streamed file in place to be re-polled rather than marking it failed.
                log.info("Server shutdown interrupted streaming; leaving file in place to be "
                        + "reprocessed on restart: " + maskURLPassword(file.toString()));
                return;
            }
            log.error("Error processing file: " + maskURLPassword(file.toString()), e);
            try {
                if (vfsConfig.getMoveAfterMoveFailure() != null) {
                    String timeStamp =
                            Utils.getSystemTime(vfsConfig.getFailedRecordTimestampFormat());
                    Utils.addFailedRecord(vfsConfig, file, timeStamp, fsManager);
                } else {
                    Utils.markFailRecord(fsManager, file, fso);
                }
            } catch (Exception failHandlingError) {
                log.error("Error in fail handling for file: " + maskURLPassword(file.toString()), failHandlingError);
                // Mark as failed record if we couldn't handle the failure
                Utils.markFailRecord(fsManager, file, fso);
            }
        } finally {
            // Release lock if file locking is enabled and we shouldn't skip
            if (fileLock) {
                lockManager.releaseLock(file);
                if (log.isDebugEnabled()) {
                    log.debug("Released the lock for file: " + maskURLPassword(file.toString()));
                }
            }
            // The lock is now released; let a waiting shutdown proceed to close the manager.
            processing = false;
        }
    }

    /**
     * Schedule retry for failed record processing
     */
    private void scheduleFailedRecordRetry(FileObject file) {
        if (failedRecordNextRetryDuration > 0) {
            retryScheduler.schedule(new FailedRecordRetryTask(file),
                    failedRecordNextRetryDuration, TimeUnit.MILLISECONDS);
            if (log.isDebugEnabled()) {
                log.debug("Scheduled retry for failed record: " + maskURLPassword(file.toString()) +
                        " after " + failedRecordNextRetryDuration + "ms");
            }
        }
    }

    /**
     * Remove file from failed records
     */
    private void removeFromFailedRecords(FileObject file) {
        // Implementation to remove from failed records tracking
        if (log.isDebugEnabled()) {
            log.debug("Removed file from failed records: " + maskURLPassword(file.toString()));
        }
        try {
            if (vfsConfig.getMoveAfterMoveFailure() != null) {
                MoveAction moveAction = new MoveAction(vfsConfig.getMoveAfterMoveFailure(), vfsConfig, fsManager);
                moveAction.execute(file);
            }
            //TODO: remove failed record from the list file.
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error moving file during failed record removal: " +
                    maskURLPassword(file.toString()), e);
        }
    }

    /**
     * Resolve hostname dynamically if enabled
     */
    private String resolveHostDynamically(String uri) {
        if (!resolveHostsDynamically) {
            return uri;
        }

        try {
            // Extract hostname from URI and resolve it
            // This is a simplified implementation
            if (uri.contains("://")) {
                String[] parts = uri.split("://");
                if (parts.length > 1) {
                    String[] hostParts = parts[1].split("/");
                    if (hostParts.length > 0) {
                        String host = hostParts[0];
                        if (host.contains("@")) {
                            host = host.substring(host.lastIndexOf("@") + 1);
                        }
                        if (host.contains(":")) {
                            host = host.substring(0, host.indexOf(":"));
                        }

                        // Resolve the hostname
                        InetAddress addr = InetAddress.getByName(host);
                        String resolvedIP = addr.getHostAddress();

                        if (log.isDebugEnabled()) {
                            log.debug("Resolved host " + host + " to " + resolvedIP);
                        }

                        // Replace hostname with resolved IP
                        return uri.replace(host, resolvedIP);
                    }
                }
            }
        } catch (UnknownHostException e) {
            log.warn("Could not resolve host dynamically for URI: " + maskURLPassword(uri), e);
        }

        return uri;
    }

    private void safeClose(FileObject fo) {
        if (fo != null) {
            try {
                fo.close();
            } catch (Exception ignore) {
            }
        }
    }

    /* =========================
                Helpers
       ========================= */

    private ResolvedFileUri extractFileUri(String propertyForUri) {
        String definedFileUri;
        switch (propertyForUri) {
            case VFSConstants.TRANSPORT_FILE_FILE_URI:
                definedFileUri = vfsConfig.getFileURI();
                break;
            default:
                definedFileUri = null;
        }
        if (StringUtils.isNotEmpty(definedFileUri)) {
            if (Utils.supportsSubDirectoryToken(definedFileUri)) {
                return new ResolvedFileUri(Utils.sanitizeFileUriWithSub(definedFileUri), true);
            } else {
                return new ResolvedFileUri(definedFileUri, false);
            }
        }
        return null;
    }

    /**
     * Check if the file/folder exists before proceeding and retrying
     */
    private FileObject initFileCheck(String fileURI) {
        boolean wasError = true;
        int retryCount = 0;

        FileObject fileObject = null;
        while (wasError) {
            try {
                if (isClosed) {
                    return null;
                }
                retryCount++;
                fileObject = fsManager.resolveFile(fileURI, fso);
                if (fileObject == null) {
                    log.error("fileObject is null");
                    throw new FileSystemException("fileObject is null");
                }
                Map<String, String> queryParams = UriParser.extractQueryParams(fileURI);
                isMounted = Boolean.parseBoolean(queryParams.get(VFSConstants.IS_MOUNTED));
                fileObject.setIsMounted(isMounted);
                wasError = false;
            } catch (FileSystemException e) {
                if (retryCount >= vfsConfig.getMaxRetryCount()) {
                    log.error("Repeatedly failed to resolve the file URI: " + maskURLPassword(fileURI), e);
                    return null;
                } else {
                    log.warn("Failed to resolve the file URI: " + maskURLPassword(fileURI) + ", in attempt "
                            + retryCount + ", " + e.getMessage() + " Retrying in " + vfsConfig.getReconnectTimeout()
                            + " milliseconds.");
                }
            }
            if (wasError) {
                try {
                    Thread.sleep(vfsConfig.getReconnectTimeout());
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    log.error("Thread was interrupted while waiting to reconnect.", e2);
                }
            }
        }
        return fileObject;
    }

    public void close() {
        isClosed = true;
        // Streaming only: signal the in-flight stream to stop cleanly and wait (bounded) for it to
        // finish so it can release its lock file while the file system manager is still open.
        // Without this, closing fsManager here would leave the .lock file behind and block
        // re-reading the file after a restart. The non-streaming path skips this entirely and
        // closes exactly as before.
        if (isRecordOrChunkStreaming()) {
            vfsConfig.setCanceled(true);
            awaitProcessingStop(SHUTDOWN_WAIT_MILLIS);
        }
        // commons-vfs 2.2 (MI 4.1.0) does not declare close() on the FileSystemManager
        // interface - it is only on DefaultFileSystemManager - so close through the
        // concrete manager we created in the constructor.
        ((StandardFileSystemManager) fsManager).close();
        if (retryScheduler != null && !retryScheduler.isShutdown()) {
            retryScheduler.shutdown();
            try {
                if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    retryScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                retryScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * True when this inbound streams in CHUNK/RECORD mode - the only case that can be interrupted
     * part-way through a file and therefore needs the graceful shutdown handling. Entire-file /
     * non-streaming returns false.
     */
    private boolean isRecordOrChunkStreaming() {
        return vfsConfig.isStreaming()
                && !StreamingConstants.STREAMING_MODE_ENTIRE_FILE
                        .equalsIgnoreCase(vfsConfig.getStreamingMode());
    }

    /**
     * Wait, up to {@code maxWaitMillis}, for the in-flight poll to finish so it can release its
     * lock before the file system manager is closed.
     */
    private void awaitProcessingStop(long maxWaitMillis) {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        while (processing && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(SHUTDOWN_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (processing) {
            log.warn("In-flight file processing did not stop within " + maxWaitMillis
                    + "ms of shutdown; closing the file system manager anyway. The file's .lock may "
                    + "remain in place. It is reclaimed automatically only when AutoLockRelease "
                    + "(transport.vfs.AutoLockRelease) is enabled; otherwise remove the .lock file "
                    + "manually before the file can be picked up again.");
        }
    }

    public void start() {
        isClosed = false;
        vfsConfig.setCanceled(false);
    }

    public void destroy() {
        unregisterShutdownHook();
        this.close();
    }

    @Override
    public void resume() {
        try {
            ((StandardFileSystemManager) fsManager).init();
            retryScheduler = Executors.newScheduledThreadPool(1);
            isClosed = false;
            vfsConfig.setCanceled(false);
        } catch (FileSystemException e) {
            log.error("Error re-initializing VFS FileSystemManager on resume", e);
        }
    }

    // Not an override on MI 4.1.0: GenericPollingConsumer declares no pause(). GenericTask
    // maps notifyLocalTaskPause() onto destroy(), which already calls close(). Kept so the
    // behaviour stays explicit and the method can be called directly.
    public void pause() {
        isClosed = true;
        this.close();
    }

    private static class ResolvedFileUri {
        final String resolvedUri;
        final boolean supportSubDirectories;

        ResolvedFileUri(String uri, boolean subDirs) {
            this.resolvedUri = uri;
            this.supportSubDirectories = subDirs;
        }
    }

    /**
     * Task to retry processing of failed records
     */
    private class FailedRecordRetryTask implements Runnable {
        private final FileObject file;

        public FailedRecordRetryTask(FileObject file) {
            this.file = file;
        }

        @Override
        public void run() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Retrying failed record: " + maskURLPassword(file.toString()));
                }

                // Check if file still exists
                if (file.exists()) {
                    // Remove from failed records and retry processing
                    removeFromFailedRecords(file);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed record file no longer exists: " + maskURLPassword(file.toString()));
                    }
                }
            } catch (Exception e) {
                log.error("Error during failed record retry: " + maskURLPassword(file.toString()), e);
                // Schedule another retry if configured
                scheduleFailedRecordRetry(file);
            }
        }
    }

}

