package org.langke;


import java.io.File;

import org.csource.common.NameValuePair;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.StorageClient1;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;


public class FastDFSClient {

	private static org.slf4j.Logger log = LoggerFactory.getLogger(FastDFSClient.class);
	private static FastDFSClient instance = null;
	private final int SLEEP = 50;
	private final int TRY_TOTAL = 5;
	
	public static FastDFSClient getInstance(){
		if(instance == null){
			instance = new FastDFSClient();
		}
		return instance;
	}
	
	private FastDFSClient() {
		try {
			String fdfsConfigPath = new ClassPathResource("fdfs_client.conf").getFile().getAbsolutePath();
			log.info("fdfs_config={}",fdfsConfigPath);
			ClientGlobal.init(fdfsConfigPath);
		} catch (Exception e) {
			log.error("",e);
		}
		log.info("fdfs_client network_timeout=" + ClientGlobal.g_network_timeout + "ms");
		log.info("fdfs_client charset=" + ClientGlobal.g_charset);
	}
	
	public String upload(byte[] localFileBuff, String ext_name){
		try {
			TrackerStorage ts = createTrackerStorage();
			StorageClient1 client = new StorageClient1(ts.trackerServer, ts.storageServer);
			NameValuePair[] metaList = null;
			String fileId = client.upload_file1(localFileBuff, ext_name, metaList);
			int try_count = TRY_TOTAL;
			while(fileId == null && try_count-- > 0){
				Thread.sleep(SLEEP);
				fileId = client.upload_file1(localFileBuff, ext_name, metaList);
			}
			if(fileId != null){
				log.info("upload success! file id is: {}" , fileId);
				return fileId;
			}else{
				log.info("upload fail ! errno {}" , (int)client.getErrorCode());
				
			}
			ts.close();
		} catch (Exception e) {
			
			log.error("",e);
		}
		return null;
	}

	public String uploadSlaveFile(byte[] localFileBuff, String master_file_id, String prefix_name){
		try {
			TrackerStorage ts = createTrackerStorage();
			StorageClient1 client = new StorageClient1(ts.trackerServer, ts.storageServer);
			String file_ext_name = master_file_id.substring(master_file_id.lastIndexOf(".")+1);
			NameValuePair[] meta_list = null;
			String slave_file_id = client.upload_file1(master_file_id, prefix_name, localFileBuff, file_ext_name, meta_list);
			int try_count = TRY_TOTAL;
			while(slave_file_id == null && try_count-- > 0){
				Thread.sleep(SLEEP);
				slave_file_id = client.upload_file1(master_file_id, prefix_name, localFileBuff, file_ext_name, meta_list);
			}
//			String generated_slave_file_id = ProtoCommon.genSlaveFilename(master_file_id, prefix_name, file_ext_name);
//			log.warn("slave_file_id equals generated_slave_file_id ? {}" , slave_file_id.equals(generated_slave_file_id));
			if(slave_file_id != null){
				log.info("upload success! file id is: {}" , slave_file_id);
				return slave_file_id;
			}else{
				log.info("upload fail ! errno {}" , (int)client.getErrorCode());
			}
			ts.close();
		} catch (Exception e) {
			log.error("",e);
		} 
		return null;
	}
	
	/**
	 * unused
	 * @param localFile
	 * @return
	 */
	public String upload(File localFile){
		try {
			TrackerStorage ts = createTrackerStorage();
			StorageClient1 client = new StorageClient1(ts.trackerServer, ts.storageServer);
			NameValuePair[] metaList = new NameValuePair[1];
			metaList[0] = new NameValuePair("File-Name", localFile.getName());
			String fileName = localFile.getName();
			String fileId = client.upload_file1(localFile.getPath(), fileName.substring(fileName.lastIndexOf(".")+1), metaList);
			int try_count = TRY_TOTAL;
			while(fileId == null && try_count-- > 0){
				Thread.sleep(SLEEP);
				fileId = client.upload_file1(localFile.getPath(), fileName.substring(fileName.lastIndexOf(".")+1), metaList);
			}
			log.info("upload success! file id is: {}" , fileId);
			ts.close();
			ts.close();
			return fileId;
		} catch (Exception e) {
			log.error("",e);
			return null;
		}
	}

	/**
	 * unused
	 * @param localFile
	 * @param master_file_id
	 * @param prefix_name
	 * @return
	 */
	public String uploadSlaveFile(File localFile, String master_file_id, String prefix_name){
		try {
			TrackerStorage ts = createTrackerStorage();
			StorageClient1 client = new StorageClient1(ts.trackerServer, ts.storageServer);
			String file_ext_name = localFile.getName().substring(localFile.getName().lastIndexOf(".")+1);
			NameValuePair[] metaList = null;
			String slave_file_id = client.upload_file1(master_file_id, prefix_name, localFile.getPath(), file_ext_name, metaList);
			int try_count = TRY_TOTAL;
			while(slave_file_id == null && try_count-- > 0){
				Thread.sleep(SLEEP);
				slave_file_id = client.upload_file1(master_file_id, prefix_name, localFile.getPath(), file_ext_name, metaList);
			}
//			String generated_slave_file_id = ProtoCommon.genSlaveFilename(master_file_id, prefix_name, file_ext_name);
//			log.warn("slave_file_id == generated_slave_file_id ? {}" , slave_file_id==generated_slave_file_id);
			if(slave_file_id != null){
				log.info("upload success! file id is: {}" , slave_file_id);
				return slave_file_id;
			}else{
				log.info("upload fail ! errno {}" , (int)client.getErrorCode());
				
			}
			ts.close();
		} catch (Exception e) {
			log.error("",e);
		}
		return null;
	}
	
	public byte[] tryDownload(String fdfs_master_id, String groupName) {
		try {
			byte[] bytes = download(fdfs_master_id, groupName);
			int count = TRY_TOTAL;
			while(bytes == null && count-- > 0){
				Thread.sleep(SLEEP);
				bytes = download(fdfs_master_id, groupName);
			}
			return bytes;
		} catch (Exception e) {
			log.error("",e);
			return null;
		}
	}
	
	private byte[] download(String fdfs_master_id, String groupName) {
		try {
			TrackerStorage ts = createTrackerStorage(groupName);
			StorageClient1 client = new StorageClient1(ts.trackerServer, ts.storageServer);
			byte[] bytes = client.download_file1(fdfs_master_id);
			ts.close();
			return bytes;
		} catch (Exception e) {
			log.error("",e);
			return null;
		}
	}
	
	private class TrackerStorage{
		public TrackerServer trackerServer;
		public StorageServer storageServer;
		public void close(){
			try {
				if(trackerServer != null){
					trackerServer.close();
				}
				if(storageServer != null){
					storageServer.close();
				}
			} catch (Exception e) {
				log.error("",e);
			}
		}
	}
	
	private TrackerStorage createTrackerStorage(){
		return createTrackerStorage(null);
	}
	
	/**
	 * groupName可为空
	 * @param groupName
	 * @return
	 */
	private TrackerStorage createTrackerStorage(String groupName){
		TrackerStorage ts = new TrackerStorage();
		try {
			TrackerClient tracker = new TrackerClient();
			TrackerServer trackerServer = tracker.getConnection();
			if (trackerServer == null){
				log.error("get tracker connection fail!");
				return null;
			}
			StorageServer storageServer = null;
			if(groupName != null ){
				storageServer = tracker.getStoreStorage(trackerServer, groupName);
			}else{
				storageServer = tracker.getStoreStorage(trackerServer);
			}
			if (storageServer == null){
				log.error("get storage connection fail!");
				return null;
			}
			ts.trackerServer = trackerServer;
			ts.storageServer = storageServer;
			log.info(ts.trackerServer.getInetSocketAddress() + " " + ts.storageServer.getInetSocketAddress());
		} catch (Exception e) {
			log.error("",e);
		}
		return ts;
	}
	
	public static void main(String[] args) {
		FastDFSClient o = FastDFSClient.getInstance();
//		File file = new File("E:/upload_server/Koala.jpg");
//		String fid = o.upload(file);
//		byte[] bytes = o.download("group1/M00/00/0A/CgoKC06bjxGgVG4GAAvqH_kipG8195.jpg");
		String fid = o.uploadSlaveFile(new File("/Users/lee/Downloads/test-oss.log"), "group1/M00/00/0A/CgoKC06bjxGgVG4GAAvqH_kipG8195.jpg", "_1000x768");
		System.out.println(fid);
	}
}
