/**
 * Copyright (c) MuleSoft, Inc. All rights reserved. http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.md file.
 */


package org.mule.module.mongo.tools;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class AbstractMongoUtility
{
    public void propagateException(Future<Void> future) throws IOException
    {
        try
        {
            future.get();
        }
        catch(InterruptedException ie)
        {
            Thread.currentThread().interrupt();
            future.cancel(true);
        }
        catch(ExecutionException ee)
        {
            throw new IOException(ee.getCause());
        }
    }

}
