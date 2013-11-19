package org.jumbodb.database.service.exporter;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.connector.importer.*;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.query.FileOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.HttpMessageConversionException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * @author Carsten Hufe
 */
public class ExportDeliveryTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(ExportDeliveryTask.class);

    private ExportDelivery exportDelivery;
    private StorageManagement storageManagement;

    public ExportDeliveryTask(ExportDelivery exportDelivery, StorageManagement storageManagement) {
        this.exportDelivery = exportDelivery;
        this.storageManagement = storageManagement;
    }

    @Override
    public void run() {
        if(exportDelivery.getState() != ExportDelivery.State.WAITING) {
            return;
        }
        JumboImportConnection imp = null;
        try {
            exportDelivery.setState(ExportDelivery.State.RUNNING);
            exportDelivery.setStatus("Sending meta data");
            List<MetaData> metaDatas = storageManagement.getMetaDataForDelivery(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion(), exportDelivery.isActivate());
            List<MetaIndex> metaIndexes = storageManagement.getMetaIndexForDelivery(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion());
            List<DataInfo> dataInfoForDelivery = storageManagement.getDataInfoForDelivery(metaDatas);
            List<IndexInfo> indexInfoForDelivery = storageManagement.getIndexInfoForDelivery(metaIndexes);
            long indexSize = getIndexSize(indexInfoForDelivery);
            long dataSize = getDataSize(dataInfoForDelivery);
            exportDelivery.setTotalBytes(indexSize + dataSize);

            imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort());
            if(imp.existsDeliveryVersion(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion())) {
                exportDelivery.setState(ExportDelivery.State.FAILED);
                exportDelivery.setStatus("Delivery version already exists on host, please delete it, before replicating.");
                return;
            }
            IOUtils.closeQuietly(imp);

            for (MetaData metaData : metaDatas) {
                imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort());
                imp.sendMetaData(metaData);
                IOUtils.closeQuietly(imp);
            }
            for (MetaIndex metaIndex : metaIndexes) {
                imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort());
                imp.sendMetaIndex(metaIndex);
                IOUtils.closeQuietly(imp);
            }
            int countDataFiles = 1;
            for (final DataInfo dataInfo : dataInfoForDelivery) {
                exportDelivery.setStatus("Copying " + countDataFiles + " of " + dataInfoForDelivery.size() + " data files");
                imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort()) {
                    @Override
                    protected void onCopyRateUpdate(long rateInBytesPerSecond, long copiedBytesSinceLastCall) {
                        if(exportDelivery.getState() != ExportDelivery.State.RUNNING) {
                            throw new IllegalStateException("State is not RUNNING -> aborted");
                        }
                        exportDelivery.setCopyRateInBytesCompressed(rateInBytesPerSecond);
                    }
                };
                long start = System.currentTimeMillis();
                imp.importData(dataInfo, new OnCopyCallback() {
                    @Override
                    public String onCopy(OutputStream outputStream) {
                        InputStream is = null;
                        DigestInputStream dis = null;
                        ExportDeliveryCountOutputStream cos = new ExportDeliveryCountOutputStream(outputStream, exportDelivery);
                        try {
                            MessageDigest md = MessageDigest.getInstance("SHA1");
                            is = storageManagement.getInputStream(dataInfo);
                            dis = new DigestInputStream(is, md);
                            IOUtils.copyLarge(dis, cos, 0l, dataInfo.getFileLength());
                            return Hex.encodeHexString(md.digest());
                        } catch (IOException e) {
                            throw new UnhandledException(e);
                        } catch (NoSuchAlgorithmException e) {
                            throw new UnhandledException(e);
                        } finally {
                            exportDelivery.addCurrentBytes(cos.getNotMeasuredBytes());
                            IOUtils.closeQuietly(cos);
                            IOUtils.closeQuietly(dis);
                            IOUtils.closeQuietly(is);
                        }
                    }
                });
                long timeDiff = System.currentTimeMillis() - start;
                if(timeDiff > 0) {
                    exportDelivery.setCopyRateInBytesUncompressed((dataInfo.getFileLength() * 1000) / timeDiff);
                    exportDelivery.setCopyRateInBytesCompressed((imp.getByteCount() * 1000) / timeDiff);
                }
                IOUtils.closeQuietly(imp);
                countDataFiles++;
            }
            int countIndexFiles = 1;
            for (final IndexInfo indexInfo : indexInfoForDelivery) {
                exportDelivery.setStatus("Copying " + countIndexFiles + " of " + dataInfoForDelivery.size() + " index files");
                imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort()) {
                    @Override
                    protected void onCopyRateUpdate(long rateInBytesPerSecond, long copiedBytesSinceLastCall) {
                        if(exportDelivery.getState() != ExportDelivery.State.RUNNING) {
                            throw new IllegalStateException("State is not RUNNING -> aborted");
                        }
                        exportDelivery.setCopyRateInBytesCompressed(rateInBytesPerSecond);
                    }
                };
                long start = System.currentTimeMillis();
                imp.importIndex(indexInfo, new OnCopyCallback() {
                    @Override
                    public String onCopy(OutputStream outputStream) {
                        InputStream is = null;
                        DigestInputStream dis = null;
                        ExportDeliveryCountOutputStream cos = new ExportDeliveryCountOutputStream(outputStream, exportDelivery);
                        try {
                            MessageDigest md = MessageDigest.getInstance("SHA1");
                            is = storageManagement.getInputStream(indexInfo);
                            dis = new DigestInputStream(is, md);
                            IOUtils.copyLarge(dis, cos, 0l, indexInfo.getFileLength());
                            return Hex.encodeHexString(md.digest());
                        } catch (IOException e) {
                            throw new UnhandledException(e);
                        } catch (NoSuchAlgorithmException e) {
                            throw new UnhandledException(e);
                        } finally {
                            exportDelivery.addCurrentBytes(cos.getNotMeasuredBytes());
                            IOUtils.closeQuietly(cos);
                            IOUtils.closeQuietly(dis);
                            IOUtils.closeQuietly(is);
                        }
                    }
                });
                long timeDiff = System.currentTimeMillis() - start;
                if(timeDiff > 0) {
                    exportDelivery.setCopyRateInBytesUncompressed((indexInfo.getFileLength() * 1000) / timeDiff);
                    exportDelivery.setCopyRateInBytesCompressed((imp.getByteCount() * 1000) / timeDiff);
                }
                IOUtils.closeQuietly(imp);
                countIndexFiles++;
            }

            imp = new JumboImportConnection(exportDelivery.getHost(), exportDelivery.getPort());
            imp.sendFinishedNotification(exportDelivery.getDeliveryChunkKey(), exportDelivery.getVersion());

            exportDelivery.setCurrentBytes(exportDelivery.getTotalBytes());
            exportDelivery.setState(ExportDelivery.State.FINISHED);
            exportDelivery.setStatus("Finished");
        } catch(Exception ex) {
            exportDelivery.setState(ExportDelivery.State.FAILED);
            exportDelivery.setStatus("Error: " + ex.getMessage());
            log.error("An error occured: ", ex);
        }
        finally {
            IOUtils.closeQuietly(imp);
        }
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
