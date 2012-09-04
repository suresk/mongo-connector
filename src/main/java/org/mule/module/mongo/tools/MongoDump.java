/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.mongo.tools;

import org.mule.module.mongo.api.MongoClient;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
import org.bson.types.BSONTimestamp;

public class MongoDump extends AbstractMongoUtility
{
    public static final String TIMESTAMP_FORMAT = "'.'yyyy-MM-dd-HH-mm";

    private MongoClient mongoClient;
    private boolean zip;
    private boolean oplog;
    private Map<String, DB> dbs = new HashMap<String, DB>();
    private DBCollection oplogCollection;
    private BSONTimestamp oplogStart;

    public MongoDump(MongoClient mongoClient)
    {
        this.mongoClient = mongoClient;
    }

    public void dump(String outputDirectory, String database, String outputName, int threads) throws IOException
    {
        Validate.notNull(outputDirectory);
        Validate.notNull(outputName);
        Validate.notNull(database);

        outputName += appendTimestamp();

        initOplog(database);

        Collection<String> collections = mongoClient.listCollections();
        if(collections != null)
        {
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            DumpWriter dumpWriter = new BsonDumpWriter(outputDirectory, outputName);
            for(String collectionName : collections)
            {
                DBCollection dbCollection = mongoClient.getCollection(collectionName);
                MongoDumpCollection dumpCollection = new MongoDumpCollection(dbCollection);
                dumpCollection.setDumpWriter(dumpWriter);

                Future<Void> future = executor.submit(dumpCollection);
                propagateException(future);
            }

            executor.shutdown();
            try
            {
                if(!executor.awaitTermination(60, TimeUnit.SECONDS))
                {
                    executor.shutdownNow();
                }

                if(oplog)
                {
                    ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
                    MongoDumpCollection dumpCollection = new MongoDumpCollection(oplogCollection);
                    dumpCollection.setName(BackupConstants.OPLOG);
                    dumpCollection.addOption(Bytes.QUERYOPTION_OPLOGREPLAY);
                    dumpCollection.addOption(Bytes.QUERYOPTION_SLAVEOK);
                    DBObject query = new BasicDBObject();
                    query.put(BackupConstants.TIMESTAMP_FIELD, new BasicDBObject("$gt", oplogStart));
                    // Filter only oplogs for given database
                    query.put(BackupConstants.NAMESPACE_FIELD, BackupUtils.getNamespacePattern(database));
                    dumpCollection.setQuery(query);
                    dumpCollection.setDumpWriter(dumpWriter);
                    Future<Void> future = singleExecutor.submit(dumpCollection);
                    propagateException(future);
                }

                if(zip)
                {
                    String dbDumpPath = outputDirectory + File.separator + outputName;
                    ZipUtils.zipDirectory(dbDumpPath);
                    FileUtils.deleteDirectory(new File(dbDumpPath));
                }
            }
            catch(InterruptedException ie)
            {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void initOplog(String database) throws IOException
    {
        if(oplog)
        {
            oplogCollection = new OplogCollection(dbs.get(BackupConstants.ADMIN_DB), dbs.get(BackupConstants.LOCAL_DB)).getOplogCollection();
            // Filter for oplogs for the given database
            DBObject query = new BasicDBObject(BackupConstants.NAMESPACE_FIELD, BackupUtils.getNamespacePattern(database));
            DBCursor oplogCursor = oplogCollection.find(query);
            oplogCursor.sort(new BasicDBObject("$natural", -1));
            if(oplogCursor.hasNext())
            {
                oplogStart = ((BSONTimestamp)oplogCursor.next().get("ts"));
            }
        }
    }

    private String appendTimestamp()
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(new Date());
    }

    public void setZip(boolean zip)
    {
        this.zip = zip;
    }

    public void setOplog(boolean oplog)
    {
        this.oplog = oplog;
    }

    public void addDB(DB db)
    {
        dbs.put(db.getName(), db);
    }
}
