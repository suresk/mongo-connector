/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.mongo.tools;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class MongoDumpCollection implements Callable<Void>
{
    private DBCollection collection;
    private DumpWriter dumpWriter;
    private DBObject query;
    private String name;
    private List<Integer> options = new ArrayList<Integer>();

    public MongoDumpCollection(DBCollection collection)
    {
        this.collection = collection;
    }

    public Void call() throws Exception
    {
        DBCursor cursor = query != null? collection.find(query) : collection.find();
        cursor.sort(new BasicDBObject("_id", 1));

        for(Integer option : options)
        {
            cursor.addOption(option);
        }

        while(cursor.hasNext())
        {
            BasicDBObject dbObject = (BasicDBObject) cursor.next();
            dumpWriter.writeObject(name != null? name : collection.getName(), dbObject);
        }
        return null;
    }

    public void setDumpWriter(DumpWriter dumpWriter)
    {
        this.dumpWriter = dumpWriter;
    }

    public void setQuery(DBObject query)
    {
        this.query = query;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void addOption(Integer option)
    {
        this.options.add(option);
    }


}
