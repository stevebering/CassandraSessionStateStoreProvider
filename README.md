CassandraSessionStateStoreProvider
==================================

An ASP.NET <a href="http://msdn.microsoft.com/en-us/library/ms178587.aspx">Session State Store Provider</a> 
implementation using <a href="http://cassandra.apache.org/">Apache Cassandra</a> for persistence.

NuGet
==================================



Configuration 
==================================
    <configuration>
      <system.web>
        <sessionState mode="Custom" customProvider="CassandraSessionStateStore">
          <providers>
            <add name="CassandraSessionStateStore" 
                 type="Cassandra.AspNet.SessionState.CassandraSessionStateStoreProvider" 
                 description="Cassandra Session State Store Provider"
                 keyspace="SessionState"
                 contactPoints="127.0.0.1,127.0.0.2"
                 useCompression="true"
                 disableBuffering="true" />                 
          </providers>
        </sessionState>
      </system.web>
    </configuration>

**keyspace**: The keyspace in which our "sessions" column family will be created. Default is *SessionState*.  
**contactPoints**: A comma-delimited list of one or more hostnames or IP addresses of Cassandra contact points. *Required*.  
**useCompression**: Turn on/off data compression on keyspace. Default is *true*.  
**disableBuffering**: Turn on/off request buffering. Default is *true*.  

Notes
=====


Instrumentation 
===============


Log4Net
=======


