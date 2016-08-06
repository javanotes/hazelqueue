package com.reactiva.hazelq.db;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
/**
 * @Experimental
 * @author esutdal
 *
 */
class Compactor implements Callable<Void> {

	private final MappedFile mapFile;
	/**
	 * 
	 * @param mapFile
	 */
	public Compactor(MappedFile mapFile){
		this.mapFile = mapFile;
	}
	private void deleteIndices(Set<Long> offsets) throws IOException 
	  {
	    String tmpFileName = mapFile.fileName+"_i_"+System.currentTimeMillis();
	    WritableByteChannel bkpFile = Channels.newChannel(new FileOutputStream(File.createTempFile(tmpFileName, null)));
	    mapFile.getIndexFile().getChannel().transferTo(0, mapFile.getIndexFile().length(), bkpFile);
	    
	    
	    
	    long pos = 0;
	    mapFile.getIndexFile().seek(pos);
	    int len = 13;
	    for(Long off : offsets)
	    {
	      pos += off;
	      mapFile.getIndexFile().seek(pos);
	      
	      //1 + 4 + array.len + 8
	      mapFile.getIndexFile().readBoolean();
	      len += mapFile.getIndexFile().readInt();
	      
	      mapFile.getIndexFile().seek(pos);//go back
	      long nextPos = pos + len;
	      
	      mapFile.getIndexFile().getChannel().transferTo(pos, mapFile.getIndexFile().length(), bkpFile);
	      
	      len = 13;
	    }
	  }
	  private void deleteData(Set<Long> offsets) throws FileNotFoundException, IOException {
	    String tmpFileName = mapFile.fileName+"_d_"+System.currentTimeMillis();
	    WritableByteChannel bkpFile = Channels.newChannel(new FileOutputStream(File.createTempFile(tmpFileName, null)));
	    mapFile.getDataFile().getChannel().transferTo(0, mapFile.getDataFile().length(), bkpFile);
	  }
	/**
	   * @WIP
	   * Compact the files by removing fragmentation caused by deleted records.
	   * This is an expensive operation and should be used with discretion.
	   * @throws IOException 
	   */
	  private void compact() throws IOException {
		mapFile.fileLock.writeLock().lock();
	    try
	    {
	      //TODO: compactor. index files have a boolean true

	      Set<Long> delIdxList = new TreeSet<>();
	      Set<Long> delDatList = new TreeSet<>();
	      
	      try(IndexIterator idxIter = new IndexIterator(mapFile))
	      {
	        if(!idxIter.isEmpty())
	        {
	          Long offset;
	          while(idxIter.hasNext())
	          {
	            offset = idxIter.next();
	            if(idxIter.isDeleted())
	            {
	              delDatList.add(offset);
	              delIdxList.add(idxIter.getIdxOffset());
	            }
	            
	                       
	          }
	        }
	      } catch (IOException e) {
	        throw e;
	      }
	      
	      deleteIndices(delIdxList);
	      deleteData(delDatList);
	      mapFile.clearCache();
	    }
	    finally
	    {
	    	mapFile.fileLock.writeLock().unlock();
	    }
	    
	  }
	  
	
	@Override
	public Void call() throws Exception {
		compact();
		return null;
	}

}
