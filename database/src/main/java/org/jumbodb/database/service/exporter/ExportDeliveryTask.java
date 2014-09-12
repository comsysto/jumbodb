package org.jumbodb.database.service.exporter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.connector.importer.*;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.management.storage.dto.deliveries.ChunkedDeliveryVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class ExportDeliveryTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(ExportDeliveryTask.class);

    private ExportDelivery exportDelivery;
    private StorageManagement storageManagement;
    private JumboImportConnection imp;

    public ExportDeliveryTask(ExportDelivery exportDelivery, StorageManagement storageManagement) {
        this.exportDelivery = exportDelivery;
        this.storageManagement = storageManagement;
    }

    @Override
    public void run() {
        if (exportDelivery.getState() != ExportDelivery.State.WAITING) {
            return;
        }
        try {
            imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort()) {
                @Override
                protected void onCopyRateUpdate(long rateInBytesPerSecond, long copiedBytesSinceLastCall) {
                    if (exportDelivery.getState() != ExportDelivery.State.RUNNING) {
                        throw new IllegalStateException("State is not RUNNING -> aborted");
                    }
                    exportDelivery.setCopyRateInBytes(rateInBytesPerSecond);
                }
            };
            exportDelivery.setState(ExportDelivery.State.RUNNING);
            exportDelivery.setStatus("Sending meta data");
            exportDelivery.setStartTimeMillis(System.currentTimeMillis());
            List<DataInfo> dataInfoForDelivery = storageManagement.getDataInfoForDelivery(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion());
            List<IndexInfo> indexInfoForDelivery = storageManagement.getIndexInfoForDelivery(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion());
            long indexSize = getIndexSize(indexInfoForDelivery);
            long dataSize = getDataSize(dataInfoForDelivery);
            exportDelivery.setTotalBytes(indexSize + dataSize);

            if (checkExistingVersion()) {
                return;
            }

            initImport();

            int countDataFiles = 1;
            for (final DataInfo dataInfo : dataInfoForDelivery) {
                exportDelivery.setStatus("Copying " + countDataFiles + " of " + dataInfoForDelivery.size() + " data files");
                importDataFile(dataInfo);
                countDataFiles++;
            }
            int countIndexFiles = 1;
            for (final IndexInfo indexInfo : indexInfoForDelivery) {
                exportDelivery.setStatus("Copying " + countIndexFiles + " of " + indexInfoForDelivery.size() + " index files");
                importIndexFile(indexInfo);
                countIndexFiles++;
            }

            commitImport();
            exportDelivery.setCurrentBytes(exportDelivery.getTotalBytes());
            exportDelivery.setState(ExportDelivery.State.FINISHED);
            exportDelivery.setStatus("Finished");
            long endTimeMillis = System.currentTimeMillis();
            long timeDiff = endTimeMillis - exportDelivery.getStartTimeMillis();
            long finalSpeed = (exportDelivery.getCurrentBytes() * 1000) / timeDiff;
            exportDelivery.setCopyRateInBytes(finalSpeed);
        } catch (Exception ex) {
            exportDelivery.setState(ExportDelivery.State.FAILED);
            exportDelivery.setStatus("Error: " + ex.getMessage());
            log.error("An error occured: ", ex);
        } finally {
            IOUtils.closeQuietly(imp);
        }
    }

    private void importIndexFile(final IndexInfo indexInfo) {
        long start = System.currentTimeMillis();
        imp.importIndexFile(indexInfo, new OnCopyCallback() {
            @Override
            public void onCopy(OutputStream outputStream) {
                InputStream is = null;
                ExportDeliveryCountOutputStream cos = new ExportDeliveryCountOutputStream(outputStream, exportDelivery);
                try {
                    is = storageManagement.getInputStream(indexInfo);
                    IOUtils.copyLarge(is, cos, 0l, indexInfo.getFileLength());
                    cos.flush();
                } catch (IOException e) {
                    throw new UnhandledException(e);
                } finally {
                    exportDelivery.addCurrentBytes(cos.getNotMeasuredBytes());
                    IOUtils.closeQuietly(is);
                }
            }
        });
//        long timeDiff = System.currentTimeMillis() - start;
//        if (timeDiff > 0) {
//            exportDelivery.setCopyRateInBytes((imp.getByteCount() * 1000) / timeDiff);
//        }

    }

    private void importDataFile(final DataInfo dataInfo) {

        long start = System.currentTimeMillis();
        imp.importDataFile(dataInfo, new OnCopyCallback() {
            @Override
            public void onCopy(OutputStream outputStream) {
                InputStream is = null;
                ExportDeliveryCountOutputStream cos = new ExportDeliveryCountOutputStream(outputStream,
                        exportDelivery);
                try {
                    is = storageManagement.getInputStream(dataInfo);
                    IOUtils.copyLarge(is, cos, 0l, dataInfo.getFileLength());
                    cos.flush();
                } catch (IOException e) {
                    throw new UnhandledException(e);
                } finally {
                    exportDelivery.addCurrentBytes(cos.getNotMeasuredBytes());
                    IOUtils.closeQuietly(is);
                }
            }
        });
//        long timeDiff = System.currentTimeMillis() - start;
//        if (timeDiff > 0) {
//            exportDelivery.setCopyRateInBytes((imp.getByteCount() * 1000) / timeDiff);
//        }
    }

    private boolean checkExistingVersion() {
        if (imp.existsDeliveryVersion(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion())) {
            exportDelivery.setState(ExportDelivery.State.FAILED);
            exportDelivery.setStatus("Delivery version already exists on host, please delete it, before replicating.");
            return true;
        }
        return false;
    }

    private void initImport() {
        ChunkedDeliveryVersion chunkedDeliveryVersion = storageManagement.getChunkedDeliveryVersion(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion());
        String info = chunkedDeliveryVersion.getInfo();
        String date = chunkedDeliveryVersion.getDate();
        imp.initImport(new ImportInfo(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion(), date, info));
    }

    private void commitImport() {
        imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort());
        imp.commitImport(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion(),
                exportDelivery.isActivateChunk(), exportDelivery.isActivateVersion());
    }

    private long getDataSize(List<DataInfo> dataInfoForDelivery) {
        long result = 0l;
        for (DataInfo dataInfo : dataInfoForDelivery) {
            result += dataInfo.getFileLength();
        }
        return result;
    }

    private long getIndexSize(List<IndexInfo> indexInfoForDelivery) {
        long result = 0l;
        for (IndexInfo indexInfo : indexInfoForDelivery) {
            result += indexInfo.getFileLength();
        }
        return result;
    }
}
