package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.lucene.search.Sort;
import org.apache.lucene.util.PriorityQueue;
import org.infinispan.Cache;
import org.infinispan.query.impl.AbstractIterator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DistributedLazyIterator extends AbstractIterator {

   private UUID queryId;

   private int fetchSize = 1;

   private int currentIndex = -1;

   // this array keeps all values (ordered) fetched by this iterator...
   private final ArrayList<Object> orderedValues = new ArrayList<Object>();

   private final HashMap<UUID, LinkedList<Object>> keysMap = new HashMap<UUID, LinkedList<Object>>();

   private Cache cache;

   public boolean hasNext = true;

   private final Sort sort;

   private HashMap<UUID, ClusteredTopDocs> topDocss;

   private PriorityQueue hq;

   private final int resultSize;

   private static final Log log = LogFactory.getLog(DistributedLazyIterator.class);

   public DistributedLazyIterator(Sort sort, int fetchSize, int resultSize, UUID id) {
      this.sort = sort;
      this.fetchSize = fetchSize;
      this.resultSize = resultSize;
      queryId = id;
   }

   public void setTopDocs(HashMap<UUID, ClusteredTopDocs> topDocss) {
      this.topDocss = topDocss;

      if (sort != null)
         hq = new FieldDocSortedHitQueue(sort.getSort(), topDocss.size());
      else
         hq = new HitQueue(topDocss.size(), false);

      // taking the first value of each queue
      for (ClusteredTopDocs ctp : topDocss.values()) {
         if (ctp.hasNext())
            hq.add(ctp.getNext());
      }

   }

   @Override
   public void close() {
      // ClusteredQuery killQuery = ClusteredQuery.destroyLazyQuery(cache, queryId);
      //
      // ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(rpcManager, cache);
      // try {
      // invoker.broadcast(killQuery);
      // } catch (Exception e) {
      // log.error("Error while broadcasting the kill query message to the cluster: {0}", e
      // .getMessage());
      // }
   }

   @Override
   public void jumpToResult(int index) throws IndexOutOfBoundsException {
      currentIndex = index;
   }

   @Override
   public void add(Object arg0) {
      throw new UnsupportedOperationException(
               "Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   @Override
   public Object next() {
      if (!hasNext())
         throw new NoSuchElementException("Out of boundaries");
      currentIndex++;
      return current();
   }

   @Override
   public int nextIndex() {
      if (!hasNext())
         throw new NoSuchElementException("Out of boundaries");
      return currentIndex + 1;
   }

   @Override
   public Object previous() {
      currentIndex--;
      return current();
   }

   private Object current() {
      // if already fecthed
      if (orderedValues.size() > currentIndex) {
         return orderedValues.get(currentIndex);
      }

      // fetch and return the value
      loadTo(currentIndex);
      return orderedValues.get(currentIndex);
   }

   private void loadTo(int index) {
      int fetched = 0;

      while (orderedValues.size() <= index || fetched < fetchSize) {
         // getting the next scoreDoc. If null, then there is no more results
         ClusteredScoreDocs scoreDoc = (ClusteredScoreDocs) hq.pop();
         if (scoreDoc == null) {
            return;
         }

         // "recharging" the queue
         ClusteredTopDocs topDoc = topDocss.get(scoreDoc.getNodeUuid());
         ClusteredScoreDocs score = topDoc.getNext();
         if (score != null) {
            hq.add(score);
         }

         // fetching the value
         ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache);
         try {
            Object value = invoker.getValue(scoreDoc.doc, topDoc.getNodeAddress(), queryId);
            orderedValues.add(value);
         } catch (Exception e) {
            log.error("Error while trying to remoting fetch next value: {0}", e.getMessage());
         }

         fetched++;
      }
   }

   @Override
   public int previousIndex() {
      return currentIndex - 1;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException(
               "Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   @Override
   public void set(Object arg0) {
      throw new UnsupportedOperationException(
               "Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   public void setQueryId(UUID queryId) {
      this.queryId = queryId;
   }

   public void setCache(Cache cache) {
      this.cache = cache;
   }

   public Cache getCache() {
      return cache;
   }

   @Override
   public boolean hasNext() {
      if (currentIndex + 1 >= resultSize) {
         return false;
      }
      return true;
   }

}