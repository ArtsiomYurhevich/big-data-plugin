/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hdfs.util;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public class NamedClusterUtil {
  public static final String HDFS_HOSTNAME = "hdfs_hostname";
  public static final String HDFS_PORT = "hdfs_port";
  public static final String SHIM_IDENTIFIER = "shim_identifier";
  public static final String HDFS_USERNAME = "username";
  public static final String HDFS_PASSWORD = "password";

  public static NamedCluster loadStoredClusterConfig( NamedClusterService namedClusterService, ObjectId id_jobentry, Repository rep,
                                                     Node entrynode, LogChannelInterface logChannelInterface ) {

    NamedCluster namedCluster = null;
    if ( entrynode != null ) {
      namedCluster = namedClusterService.getClusterTemplate();
      namedCluster.setHdfsHost( XMLHandler.getTagValue( entrynode, HDFS_HOSTNAME ) ); //$NON-NLS-1$
      namedCluster.setHdfsPort( XMLHandler.getTagValue( entrynode, HDFS_PORT ) ); //$NON-NLS-1$
      namedCluster.setHdfsPort( XMLHandler.getTagValue( entrynode, SHIM_IDENTIFIER ) ); //$NON-NLS-1$
      namedCluster.setHdfsPort( XMLHandler.getTagValue( entrynode, HDFS_USERNAME ) ); //$NON-NLS-1$
      namedCluster.setHdfsPort( XMLHandler.getTagValue( entrynode, HDFS_PASSWORD ) ); //$NON-NLS-1$
    } else if ( rep != null ) {
      try {
        namedCluster = namedClusterService.getClusterTemplate();
        namedCluster.setHdfsHost( rep.getJobEntryAttributeString( id_jobentry, HDFS_HOSTNAME ) );
        namedCluster.setHdfsPort( rep.getJobEntryAttributeString( id_jobentry, HDFS_PORT ) ); //$NON-NLS-1$
        namedCluster.setHdfsPort( rep.getJobEntryAttributeString( id_jobentry, SHIM_IDENTIFIER ) ); //$NON-NLS-1$
        namedCluster.setHdfsPort( rep.getJobEntryAttributeString( id_jobentry, HDFS_USERNAME ) ); //$NON-NLS-1$
        namedCluster.setHdfsPort( rep.getJobEntryAttributeString( id_jobentry, HDFS_PASSWORD ) ); //$NON-NLS-1$
      } catch ( KettleException ke ) {
        logChannelInterface.logError( ke.getMessage(), ke );
      }
    }
    return namedCluster;
  }

  public static NamedCluster saveClusterConfig( StringBuilder xml, String clusterName, NamedClusterService namedClusterService, NamedCluster storedCluster ) {

    IMetaStore metaStore = getLocalMetaStore();

    NamedCluster namedCluster = namedClusterService.getNamedClusterByName( clusterName, metaStore );
    if ( namedCluster == null ) {
      namedCluster = storedCluster;
    }

    xml.append( "      " ).append( XMLHandler.addTagValue( HDFS_HOSTNAME, namedCluster.getHdfsHost() ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( HDFS_PORT, namedCluster.getHdfsPort() ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( SHIM_IDENTIFIER, namedCluster.getShimIdentifier() ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( HDFS_USERNAME, namedCluster.getHdfsUsername() ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( HDFS_PASSWORD, namedCluster.getHdfsPassword() ) );

    return namedCluster;
  }

  public static NamedCluster saveClusterConfig( String sourceConfigurationName, NamedCluster storedCluster, NamedClusterService namedClusterService,
                                        Repository rep, ObjectId id_trans, ObjectId id_step, IMetaStore metaStore ) throws KettleException {

    NamedCluster namedCluster = namedClusterService.getNamedClusterByName( sourceConfigurationName, metaStore );

    if ( namedCluster == null ) {
      namedCluster = storedCluster;
    }

    rep.saveJobEntryAttribute( id_trans, id_step, HDFS_HOSTNAME, namedCluster.getHdfsHost() ); // $NON-NLS-1$
    rep.saveJobEntryAttribute( id_trans, id_step, HDFS_PORT, namedCluster.getHdfsPort() ); // $NON-NLS-1$
    rep.saveJobEntryAttribute( id_trans, id_step, SHIM_IDENTIFIER, namedCluster.getShimIdentifier() ); // $NON-NLS-1$
    rep.saveJobEntryAttribute( id_trans, id_step, HDFS_USERNAME, namedCluster.getHdfsUsername() ); // $NON-NLS-1$
    rep.saveJobEntryAttribute( id_trans, id_step, HDFS_PASSWORD, namedCluster.getHdfsPassword() ); // $NON-NLS-1$

    return namedCluster;
  }

  private static IMetaStore getLocalMetaStore() {
    IMetaStore metaStore = null;
    try {
      metaStore = MetaStoreConst.openLocalPentahoMetaStore();
    } catch ( Throwable e ) {
      metaStore = null;
    }
    return metaStore;
  }
}
