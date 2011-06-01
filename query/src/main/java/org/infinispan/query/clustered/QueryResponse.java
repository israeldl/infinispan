package org.infinispan.query.clustered;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import org.apache.lucene.search.TopDocs;
import org.infinispan.remoting.transport.Address;

/**
 * 
 * QueryResponse.
 * 
 * A response of a request of more results to a lazy iterator
 * 
 * @author israel
 * @since 4.0
 */
public class QueryResponse implements Serializable {

   private static final long serialVersionUID = -2113889511877165954L;

   private List<Object> keys;

   private final UUID nodeUUID;

   private TopDocs topDocs;

   private Address address;

   QueryResponse(List<Object> keys, UUID nodeUUid) {
      this.keys = keys;
      this.nodeUUID = nodeUUid;
   }

   public TopDocs getTopDocs() {
      return topDocs;
   }

   public void setKeys(List<Object> keys) {
      this.keys = keys;
   }

   public QueryResponse(TopDocs topDocs, UUID nodeUUid) {
      this.keys = null;
      this.nodeUUID = nodeUUid;
      this.topDocs = topDocs;
   }

   QueryResponse(Object value, UUID nodeUUid, int docId) {
      nodeUUID = nodeUUid;
      keys = null;
   }

   public List<Object> getKeys() {
      return keys;
   }

   public UUID getNodeUUID() {
      return nodeUUID;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public Address getAddress() {
      return address;
   }

}