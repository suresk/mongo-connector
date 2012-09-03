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
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.Validate;

public class MongoRestoreDirectory implements Callable<Void>
{
    private MongoClient mongoClient;
    private boolean drop;
    private boolean oplogReplay;
    private boolean applyIncrementals;
    private String inputPath;


    public Void call() throws Exception
    {
        restore();
        return null;
    }

    private void restore() throws IOException
    {
        Validate.notNull(inputPath);
        List<RestoreFile> restoreFiles = getRestoreFiles(inputPath);
        RestoreFile oplogRestore = null;
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
                oplogRestore = restoreFile;
            }
        }
        if((applyIncrementals || oplogReplay) && oplogRestore != null)
        {
            mongoClient.executeComamnd(new BasicDBObject("applyOps", oplogRestore.getCollectionObjects().toArray()));
        }

    }


        private List<RestoreFile> getRestoreFiles(String inputPath) throws IOException
    {
        List<RestoreFile> restoreFiles = new ArrayList<RestoreFile>();
        File input = new File(inputPath);
        if(ZipUtils.isZipFile(input))
        {
            File unzippedFolder = new File(BackupUtils.removeExtension(inputPath));
            org.mule.util.FileUtils.unzip(input, unzippedFolder);
            input = unzippedFolder;
        }

        if(input.isDirectory())
        {
            for(Object file : FileUtils.listFiles(input, FileFilterUtils.suffixFileFilter("bson"), null))
            {
                restoreFiles.add(new RestoreFile((File)file));
            }
        }
        else if(BackupUtils.isBsonFile(input))
        {
            restoreFiles.add(new RestoreFile(input));
        }
        return restoreFiles;
    }

    private boolean isOplog(String collection)
    {
        return collection.startsWith(BackupConstants.OPLOG + ".");
    }

    public void setDrop(boolean drop)
    {
        this.drop = drop;
    }

    public void setOplogReplay(boolean oplogReplay)
    {
        this.oplogReplay = oplogReplay;
    }

    public void setApplyIncrementals(boolean applyIncrementals)
    {
        this.applyIncrementals = applyIncrementals;
    }

    public void setInputPath(String inputPath)
    {
        this.inputPath = inputPath;
    }

    public void setMongoClient(MongoClient mongoClient)
    {
        this.mongoClient = mongoClient;
    }
}
