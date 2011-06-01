/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.clustered;

import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.search.engine.SearchFactoryImplementor;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.Cache;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * 
 * Keep the active lazy iterators. Each node has his own box.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class QueryBox {

   // <query UUID, ISPNQuery>
   private final ConcurrentHashMap<UUID, DocumentExtractor> queries = new ConcurrentHashMap<UUID, DocumentExtractor>();

   // queries UUIDs ordered
   private final LinkedList<UUID> ageOrderedKeys = new LinkedList<UUID>();

   // For eviction. Probably there is a better way...
   private static final int BOX_LIMIT = 3000;

   // this id will be sent with the responses
   private final UUID myId = UUID.randomUUID();

   private SearchFactoryImplementor searchFactoryImplementor;

   private Cache cache;

   public org.infinispan.util.logging.Log log;

   public Object getValue(UUID uid, int docIndex) {
      touch(uid);

      DocumentExtractor extractor = queries.get(uid);

      String bufferDocumentId;
      try {
         bufferDocumentId = (String) extractor.extract(docIndex).getId();
      } catch (IOException e) {
         // FIXME
         log.error("Error", e);
         return null;
      }
      Object value = cache.get(KeyTransformationHandler.stringToKey(bufferDocumentId));

      return value;
   }

   // public List<Object> getKeys(UUID uid, ScoreDoc[] docs) {
   // Searcher searcher = queries.get(uid);
   //
   // List<Object> keys = new ArrayList<Object>(docs.length);
   //
   // for (ScoreDoc doc : docs) {
   // keys.add(getKey(searcher, doc.doc));
   // }
   //
   // return keys;
   // }

   // private Object getKey(Searcher searcher, int docIndex) {
   // Document doc;
   // try {
   // doc = searcher.doc(docIndex);
   // } catch (CorruptIndexException e) {
   // log.error("Error while trying to get more results... {0}", e);
   // return null;
   // } catch (IOException e) {
   // log.error("Error while trying to get more results... {0}", e);
   // return null;
   // }
   // Class clazz = DocumentBuilderIndexedEntity.getDocumentClass(doc);
   // String id = (String) DocumentBuilderIndexedEntity.getDocumentId(
   // getSearchFactoryImplementor(), clazz, doc);
   // Object key = KeyTransformationHandler.stringToKey(id);
   // return key;
   // }

   private void touch(UUID id) {
      synchronized (ageOrderedKeys) {
         ageOrderedKeys.remove(id);
         ageOrderedKeys.addFirst(id);
      }
   }

   public void kill(UUID id) {
      DocumentExtractor extractor = queries.remove(id);
      if (extractor != null)
         extractor.close();
   }

   public synchronized void put(UUID id, DocumentExtractor extractor) {
      synchronized (ageOrderedKeys) {
         if (ageOrderedKeys.size() >= BOX_LIMIT) {
            ageOrderedKeys.removeLast();
         }
         ageOrderedKeys.add(id);
      }

      queries.put(id, extractor);
   }

   public UUID getMyId() {
      return myId;
   }

   public void setSearchFactoryImplementor(SearchFactoryImplementor searchFactoryImplementor) {
      this.searchFactoryImplementor = searchFactoryImplementor;
   }

   public SearchFactoryImplementor getSearchFactoryImplementor() {
      return searchFactoryImplementor;
   }

   public void setCache(Cache cache) {
      this.cache = cache;
   }

}
