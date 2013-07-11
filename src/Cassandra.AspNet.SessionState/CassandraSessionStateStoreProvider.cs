using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Configuration;
using System.Web.SessionState;
using log4net;

namespace Cassandra.AspNet.SessionState
{
    /// <summary>
    /// An ASP.NET <a href="http://msdn.microsoft.com/en-us/library/ms178587.aspx">Session State Store Provider</a>
    /// implementation using <a href="http://cassandra.apache.org/">Apache Cassandra</a> for persistence.
    /// </summary>
    public class CassandraSessionStateStoreProvider
        : SessionStateStoreProviderBase, IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(CassandraSessionStateStoreProvider));
        private string _applicationName;
        private Cluster _cluster;
        private Session _session;
        private ClusterOptions _options;
        private TimeSpan _timeout;

        /// <summary>
        /// The ApplicationName property is used to differentiate sessions in the data source by application.
        /// </summary>
        public string ApplicationName {
            get { return _applicationName; }
        }

        /// <summary>
        /// Returns a reference to the Cassandra cluster used to persist session state.
        /// </summary>
        /// <returns></returns>
        private Cluster GetCluster() {
            if (_cluster == null) {
                var builder = Cluster.Builder();

                if (_options.UseCompression) {
                    Logger.Debug("Using Compression");
                    builder.WithCompression(CompressionType.Snappy);
                }

                if (_options.UseNoBuffering) {
                    Logger.Debug("No buffering");
                    builder.WithoutRowSetBuffering();
                }

                builder.AddContactPoints(_options.ContactPoints);
                _cluster = builder.Build();
            }

            return _cluster;
        }

        /// <summary>
        /// Returns a reference to the Cassandra session used to communicate with the Cassandra cluster
        /// </summary>
        /// <returns></returns>
        private Session Connect() {
            if (_session == null) {
                _session = GetCluster().Connect();
                _session.CreateKeyspaceIfNotExists(_options.Keyspace);
                _session.ChangeKeyspace(_options.Keyspace);
            }

            return _session;
        }

        public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config) {
            // initialize values from web.config 
            if (config == null) {
                throw new ArgumentNullException("config");
            }

            Logger.DebugFormat("Beginning Initialize. Name: {0}. Config: {1}.",
                            name,
                            config.AllKeys.Aggregate("", (aggregate, next) => aggregate + next + ":" + config[next]));

            if (name.Length == 0) {
                name = "CassandraSessionStateStoreProvider";
            }

            if (string.IsNullOrEmpty(config["description"])) {
                config.Remove("description");
                config.Add("description", "Cassandra Session State Store Provider");
            }

            // initialize the abstract base class.
            base.Initialize(name, config);

            // initialize the ApplicationName property.
            _applicationName = System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath;

            // get the <sessionState> configuration element.
            var cfg = WebConfigurationManager.OpenWebConfiguration(ApplicationName);
            var settings = (SessionStateSection)cfg.GetSection("system.web/sessionState");
            _timeout = settings.Timeout;

            // initialize our options
            var options = ClusterOptions.Default;

            // determine which hosts we will be using as contact points
            var contactPointsSetting = config["contactPoints"];
            if (contactPointsSetting != null) {
                var contactPointsCollection = contactPointsSetting.Trim().Split(',');
                var contactPoints = contactPointsCollection.Select(x => x.Trim()).ToArray();
                options.ContactPoints = contactPoints;
            }

            // determine if compression will be used
            var compressionSetting = config["useCompression"];
            bool useCompression;
            if (compressionSetting != null && bool.TryParse(compressionSetting, out useCompression)) {
                options.UseCompression = useCompression;
            }

            // determine if we want to turn off buffering
            var bufferingSetting = config["disableBuffering"];
            bool disableBuffering;
            if (bufferingSetting != null && bool.TryParse(bufferingSetting, out disableBuffering)) {
                options.UseNoBuffering = disableBuffering;
            }

            this._options = options;

            // create our schema, if it does not already exist
            RefreshSchema();

            Logger.Debug("Completed Initialize");
        }

        private void RefreshSchema() {
            var session = Connect();

            try {
                Logger.Debug("Creating schema...");

                session.Cluster.WaitForSchemaAgreement(
                    session.Execute(Queries.Schema)
                    );

                Logger.Info("Created schema.");
            }
            catch (AlreadyExistsException) {
                // if we have run before, this is expected
                Logger.Debug("Schema already existed.");
            }
        }

        /// <summary>
        /// Retrieves session values and information from the session data store and locks the session-item data 
        /// at the data store for the duration of the request. 
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        /// <param name="locked">An output parameter indicating whether the item is currently exclusively locked.</param>
        /// <param name="lockAge">The age of the exclusive lock (if present)</param>
        /// <param name="lockId">The identifier of the exclusive lock (if present)</param>
        /// <param name="actions">Used with sessions whose Cookieless property is true, 
        /// when the regenerateExpiredSessionId attribute is set to true. 
        /// An actionFlags value set to InitializeItem (1) indicates that the entry in the session data store is a 
        /// new session that requires initialization.</param>
        /// <returns>The session data</returns>
        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions) {
            try {
                Logger.DebugFormat("Beginning GetItemExclusive. SessionId: {0}.", id);

                var item = GetSessionStoreItem(true, context, id, out locked, out lockAge, out lockId, out actions);

                Logger.DebugFormat(
                    "Completed GetItemExclusive. SessionId: {0}, locked: {1}, lockAge: {2}, lockId: {3}, actions: {4}.",
                    id, locked, lockAge, lockId, actions);

                return item;
            }
            catch (Exception ex) {
                Logger.Error(
                    string.Format("Error during GetItemExclusive. SessionId: {0}.", id), ex);
                throw;
            }
        }

        /// <summary>
        /// This method performs the same work as the GetItemExclusive method, except that it does not attempt to lock the session item in the data store.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        /// <param name="locked">An output parameter indicating whether the item is currently exclusively locked.</param>
        /// <param name="lockAge">The age of the exclusive lock (if present)</param>
        /// <param name="lockId">The identifier of the exclusive lock (if present)</param>
        /// <param name="actions">Used with sessions whose Cookieless property is true, 
        /// when the regenerateExpiredSessionId attribute is set to true. 
        /// An actionFlags value set to InitializeItem (1) indicates that the entry in the session data store is a 
        /// new session that requires initialization.</param>
        /// <returns>The session data</returns>
        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions) {
            try {
                Logger.DebugFormat("Beginning GetItemExclusive. SessionId: {0}.", id);

                var item = GetSessionStoreItem(false, context, id, out locked, out lockAge, out lockId, out actions);

                Logger.DebugFormat(
                    "Completed GetItemExclusive. SessionId: {0}, locked: {1}, lockAge: {2}, lockId: {3}, actions: {4}.",
                    id, locked, lockAge, lockId, actions);

                return item;
            }
            catch (Exception ex) {
                Logger.Error(
                    string.Format("Error during GetItemExclusive. SessionId: {0}.", id), ex);
                throw;
            }
        }

        private SessionStateStoreData GetSessionStoreItem(bool exclusive, HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions) {
            throw new NotImplementedException();
        }
    }

    public class ClusterOptions
    {
        public static ClusterOptions Default = new ClusterOptions {
            UseCompression = true,
            UseNoBuffering = true,
            ContactPoints = new[] { "localhost" },
            Keyspace = "SessionState",
        };

        public bool UseCompression { get; set; }
        public bool UseNoBuffering { get; set; }
        public string[] ContactPoints { get; set; }
        public string Keyspace { get; set; }
    }

    public static class Queries
    {
        public static string Schema = @"CREATE TABLE session (
            id text,
            appName text,
            created timestamp,
            expires timestamp,
            lockDate timestamp,
            lockId int,
            timeout int,
            locked boolean,
            sessionItems blob,
            flags int,
            PRIMARY KEY(id, appName) );";

        public static string Select = @"";
        public static string Insert = @"";
    }
}
