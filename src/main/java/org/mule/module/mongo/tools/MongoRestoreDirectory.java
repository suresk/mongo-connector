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
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.lang.Validate;

public class MongoRestoreDirectory implements Callable<Void>
{
    private MongoClient mongoClient;
    private boolean drop;
    private boolean oplogReplay;
    private String inputPath;
    private String database;

    public Void call() throws Exception
    {
        restore();
        return null;
    }

    private void restore() throws IOException
    {
        Validate.notNull(inputPath);
        List<RestoreFile> restoreFiles = getRestoreFiles(inputPath);
        List<RestoreFile> oplogRestores = new ArrayList<RestoreFile>();
        for(RestoreFile restoreFile : restoreFiles)
        {
            if(!isOplog(restoreFile.getCollection()))
            {
                if(drop)
                {
                    // System collections cannot be dropped
                    if(!BackupUtils.isSystemCollection(restoreFile.getCollection()))
                    {
                        mongoClient.dropCollection(restoreFile.getCollection());
                    }
                }

                DBCollection dbCollection = mongoClient.getCollection(restoreFile.getCollection());
                List<DBObject> dbObjects = restoreFile.getCollectionObjects();

                if(BackupUtils.isUserCollection(restoreFile.getCollection()))
                {
                    for(DBObject currentUser : dbCollection.find())
                    {
                        if(!dbObjects.contains(currentUser))
                        {
                            dbCollection.remove(currentUser);
                        }
                    }
                }

                for(DBObject dbObject : dbObjects)
                {
                    dbCollection.save(dbObject);
                }
            }
            else
            {
                oplogRestores.add(restoreFile);
            }
        }
        if(oplogReplay && !oplogRestores.isEmpty())
        {
            for(RestoreFile oplogRestore : oplogRestores)
            {
                mongoClient.executeComamnd(new BasicDBObject("applyOps", filterOplogForDatabase(oplogRestore).toArray()));
            }
        }
    }

    private List<DBObject> filterOplogForDatabase(RestoreFile oplogFile) throws IOException
    {
        List<DBObject> oplogEntries = oplogFile.getCollectionObjects();
        List<DBObject> dbOplogEntries = new ArrayList<DBObject>();

        for(DBObject oplogEntry : oplogEntries)
        {
            if(((String)oplogEntry.get(BackupConstants.NAMESPACE_FIELD)).startsWith(database + "."))
            {
                dbOplogEntries.add(oplogEntry);
            }
        }

        return dbOplogEntries;
    }

    private void processRestoreFiles(File input, List<RestoreFile> restoreFiles) throws IOException
    {
        if(ZipUtils.isZipFile(input))
        {
            File unzippedFolder = new File(BackupUtils.removeExtension(input.getPath()));
            org.mule.util.FileUtils.unzip(input, unzippedFolder);
            input = unzippedFolder;
        }

        if(input.isDirectory())
        {
            for(File file : input.listFiles())
            {
                processRestoreFiles(file, restoreFiles);
            }
        }
        else if(BackupUtils.isBsonFile(input))
        {
            restoreFiles.add(new RestoreFile(input));
        }
    }

    private List<RestoreFile> getRestoreFiles(String inputPath) throws IOException
    {
        List<RestoreFile> restoreFiles = new ArrayList<RestoreFile>();
        processRestoreFiles(new File(inputPath), restoreFiles);
        Collections.sort(restoreFiles);
        return restoreFiles;
    }

    private boolean isOplog(String collection)
    {
        return collection.startsWith(BackupConstants.OPLOG);
    }

    public void setDrop(boolean drop)
    {
        this.drop = drop;
    }

    public void setOplogReplay(boolean oplogReplay)
    {
        this.oplogReplay = oplogReplay;
    }

    public void setInputPath(String inputPath)
    {
        this.inputPath = inputPath;
    }

    public void setMongoClient(MongoClient mongoClient)
    {
        this.mongoClient = mongoClient;
    }

    public void setDatabase(String database)
    {
        this.database = database;
    }

}
