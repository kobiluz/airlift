package org.eclipse.jetty.client;

import org.eclipse.jetty.client.api.Connection;

import java.util.Collection;

public final class ConnectionPoolAccessor
{
    private ConnectionPoolAccessor() {}

    public static Collection<Connection> getActiveConnections(AbstractConnectionPool pool)
    {
        return pool.getActiveConnections();
    }

    public static Collection<Connection> getIdleConnections(AbstractConnectionPool pool)
    {
        return pool.getIdleConnections();
    }
}
