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

import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.query.ISPNQuery;
import org.infinispan.query.clustered.commandworkers.CQCreateLazyQuery;
import org.infinispan.query.clustered.commandworkers.CQLazyFetcher;
import org.infinispan.query.clustered.commandworkers.ClusteredQueryCommandWorker;

public enum ClusteredQueryCommandType {

   GET_ALL_KEY_LIST(CQCreateLazyQuery.class), CREATE_LAZY_SEARCHER(CQCreateLazyQuery.class), DESTROY_SEARCHER(
            CQCreateLazyQuery.class), GET_SOME_KEYS(CQLazyFetcher.class), GET_RESULT_SIZE(
            CQCreateLazyQuery.class);

   private Class clazz;

   private ClusteredQueryCommandType(Class clazz) {
      this.clazz = clazz;
   }

   public ClusteredQueryCommandWorker getCommand(Cache cache, ISPNQuery query,UUID lazyQueryId, int docIndex) {
      ClusteredQueryCommandWorker command = null;
      try {
         command = (ClusteredQueryCommandWorker) clazz.newInstance();
      } catch (InstantiationException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      command.init(cache, query,lazyQueryId, docIndex);
      return command;
   }

}
