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
                 contactPoints="127.0.0.1,127.0.0.2"
                 useCompression="true"
                 disableBuffering="true" />                 
          </providers>
        </sessionState>
      </system.web>
    </configuration>

Notes
=====


Instrumentation 
===============


Log4Net
=======


