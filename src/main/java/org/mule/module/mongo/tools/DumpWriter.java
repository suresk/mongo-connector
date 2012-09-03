/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */

package org.mule.module.mongo.tools;

import com.mongodb.DBObject;

import java.io.File;
import java.io.IOException;

public abstract class DumpWriter
{
    private String outputDirectory;
    private String database;

    public DumpWriter(String outputDirectory, String database)
    {
        this.outputDirectory = outputDirectory;
        this.database = database;
    }

    public DumpWriter(String outputDirectory)
    {
        this.outputDirectory = outputDirectory;
    }

    public String getFilePath(String collection)
    {
        StringBuilder path = new StringBuilder(outputDirectory);
        path.append(File.separator);

        if(database != null)
        {
            path.append(database)
                .append(File.separator);

        }
            path.append(collection)
                .append(".")
                .append(getExtension());
        return path.toString();
    }

    public abstract String getExtension();

    public abstract void writeObject(String collection, DBObject dbObject) throws IOException;

}
