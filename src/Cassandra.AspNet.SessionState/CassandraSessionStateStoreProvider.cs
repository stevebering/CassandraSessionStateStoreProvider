using System;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Configuration;
using System.Web.SessionState;
using Cassandra.Data.Linq;
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
        private SessionStateContext _context;
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
            _context = new SessionStateContext(Connect());

            Logger.Debug("Completed Initialize");
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

        public override void Dispose() {
            _context = null;
            _session = null;
            _cluster = null;
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

        private ContextTable<UserSession> GetUserSessionsTable() {
            var t = _context.GetTable<UserSession>();
            t.CreateIfNotExists();
            _context.Attach(t, EntityUpdateMode.ModifiedOnly, EntityTrackingMode.KeepAttachedAfterSave);
            return t;
        }

        /// <summary>
        /// If the newItem parameter is true, the SetAndReleaseItemExclusive method inserts a new item into the data store with the supplied values. 
        /// Otherwise, the existing item in the data store is updated with the supplied values, and any lock on the data is released. 
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        /// <param name="item">The current session values to be stored</param>
        /// <param name="lockId">The lock identifier for the current request.</param>
        /// <param name="newItem">If true, a new item is inserted into the store.  Otherwise, the existing item in 
        /// the data store is updated with the supplied values, and any lock on the data is released. </param>
        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem) {
            try {
                Logger.DebugFormat("Beginning SetAndReleaseItemExclusive. SessionId: {0}, LockId: {1}, newItem: {2}.",
                                   id, lockId, newItem);

                var serializedItems = Serialize((SessionStateItemCollection)item.Items);
                var table = GetUserSessionsTable();

                UserSession userSession;

                if (newItem) {
                    userSession = table.FirstOrDefault(x => x.SessionId == id &&
                                                            x.ApplicationName == ApplicationName &&
                                                            x.Expires < DateTime.UtcNow)
                                                            .Execute();
                    if (userSession != null) {
                        throw new InvalidOperationException(
                            string.Format("Item already exists with SessionId: {0} and ApplicationName: {1}.", id,
                                          this.ApplicationName));
                    }

                    userSession = new UserSession(id, ApplicationName);
                    table.AddNew(userSession, EntityTrackingMode.KeepAttachedAfterSave);
                }
                else {
                    userSession = table.FirstOrDefault(x => x.SessionId == id &&
                                                            x.ApplicationName == ApplicationName &&
                                                            x.LockId == (int)lockId)
                                                            .Execute();
                }

                var expires = DateTime.UtcNow.AddMinutes(_timeout.TotalMinutes);
                userSession.Expires = expires;
                userSession.SessionItems = serializedItems;
                userSession.Locked = false;

                _context.SaveChanges(SaveChangesMode.Batch);

                Logger.DebugFormat("Completed SetAndReleaseItemExclusive. SessionId: {0}, LockId: {1}, newItem: {2}.",
                                   id, lockId, newItem);
            }
            catch (Exception ex) {
                Logger.Error(string.Format(
                        "Error during SetAndReleaseItemExclusive. SessionId: {0}, LockId: {1}, newItem: {2}.", id,
                        lockId, newItem), ex);
                throw;
            }
        }

        /// <summary>
        /// Releases the lock on an item in the session data store.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        /// <param name="lockId">The lock identifier for the current request.</param>
        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId) {
            try {
                Logger.DebugFormat("Beginning ReleaseItemExclusive. SessionId: {0}, LockId: {1}.", id, lockId);

                var table = GetUserSessionsTable();

                var sessionState = table.FirstOrDefault(x => x.SessionId == id &&
                                                             x.ApplicationName == ApplicationName &&
                                                             x.LockId == (int)lockId)
                                                             .Execute();

                sessionState.Locked = false;

                var expires = DateTime.UtcNow.AddMinutes(_timeout.TotalMinutes);
                sessionState.Expires = expires;

                _context.SaveChanges(SaveChangesMode.Batch);

                Logger.DebugFormat("Completed ReleaseItemExclusive. SessionId: {0}, LockId: {1}", id, lockId);
            }
            catch (Exception ex) {
                Logger.Error(string.Format(
                        "Error during ReleaseItemExclusive. SessionId: {0}, LockId: {1}.", id,
                        lockId), ex);
                throw;
            }
        }

        /// <summary>
        /// deletes the session information from the data store where the data store item matches the supplied SessionID value, 
        /// the current application, and the supplied lock identifier.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        /// <param name="lockId">The exclusive-lock identifier.</param>
        /// <param name="item"></param>
        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item) {
            try {
                Logger.DebugFormat("Beginning RemoveItem. SessionId: {0}, LockId: {1}.", id, lockId);

                var table = GetUserSessionsTable();
                var sessionState = table.FirstOrDefault(x => x.SessionId == id &&
                                                             x.ApplicationName == ApplicationName &&
                                                             x.LockId == (int)lockId)
                                                             .Execute();

                if (sessionState != null) {
                    table.Delete(sessionState);
                    _context.SaveChanges(SaveChangesMode.Batch);
                }

                Logger.DebugFormat("Completed RemoveItem. SessionId: {0}, LockId: {1}.", id, lockId);
            }
            catch (Exception ex) {
                Logger.Error(string.Format("Error during RemoveItem. SessionId: {0}; LockId: {1}", id, lockId), ex);
                throw;
            }
        }

        /// <summary>
        /// Resets the expiry timeout for a session item.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        public override void ResetItemTimeout(HttpContext context, string id) {
            try {
                Logger.DebugFormat("Beginning ResetItemTimeout. SessionId: {0}.", id);

                var table = GetUserSessionsTable();
                var sessionState = table.FirstOrDefault(x => x.SessionId == id &&
                                                             x.ApplicationName == ApplicationName)
                                                             .Execute();

                if (sessionState != null) {
                    var expiry = DateTime.UtcNow.AddMinutes(_timeout.TotalMinutes);
                    sessionState.Expires = expiry;
                    _context.SaveChanges();
                }

                Logger.DebugFormat("Completed ResetItemTimeout. SessionId: {0}.", id);
            }
            catch (Exception ex) {
                Logger.Error(string.Format("Error during ResetItemTimeout. SessionId: {0}.", id), ex);
                throw;
            }
        }

        /// <summary>
        /// Adds an uninitialized item to the session data store.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="id">The session identifier.</param>
        /// <param name="timeout">The expiry timeout in minutes.</param>
        public override void CreateUninitializedItem(HttpContext context, string id, int timeout) {
            try {
                Logger.DebugFormat("Beginning CreateUninitializedItem. SessionId: {0}, Timeout: {1}.", id, timeout);

                var expiry = DateTime.UtcNow.AddMinutes(timeout);

                var sessionState = new UserSession(id, ApplicationName) {
                    Expires = expiry
                };

                var table = GetUserSessionsTable();
                table.AddNew(sessionState, EntityTrackingMode.KeepAttachedAfterSave);
                _context.SaveChanges(SaveChangesMode.Batch);

                Logger.DebugFormat("Completed CreateUninitializedItem. SessionId: {0}, Timeout: {1}.", id, timeout);

            }
            catch (Exception ex) {
                Logger.Error(string.Format("Error during CreateUninitializedItem. SessionId: {0}, Timeout: {1}.", id, timeout), ex);
                throw;
            }
        }

        /// <summary>
        ///  returns a new SessionStateStoreData object with an empty ISessionStateItemCollection object, 
        ///  an HttpStaticObjectsCollection collection, and the specified Timeout value.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        /// <param name="timeout">The expiry timeout in minutes.</param>
        /// <returns>A newly created SessionStateStoreData object.</returns>
        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout) {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                                 SessionStateUtility.GetSessionStaticObjects(context),
                                 timeout);
        }

        /// <summary>
        /// Takes as input a delegate that references the Session_OnEnd event defined in the Global.asax file. 
        /// If the session-state store provider supports the Session_OnEnd event, a local reference to the 
        /// SessionStateItemExpireCallback parameter is set and the method returns true; otherwise, the method returns false.
        /// </summary>
        /// <param name="expireCallback">A callback.</param>
        /// <returns>False.</returns>
        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback) {
            return false;
        }

        /// <summary>
        /// Performs any initialization required by your session-state store provider.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        public override void InitializeRequest(HttpContext context) {
        }

        /// <summary>
        /// Performs any cleanup required by your session-state store provider.
        /// </summary>
        /// <param name="context">The HttpContext instance for the current request</param>
        public override void EndRequest(HttpContext context) {
        }

        /// <summary>
        /// GetSessionStoreItem is called by both the GetItem and 
        /// GetItemExclusive methods. GetSessionStoreItem retrieves the 
        /// session data from the data source. If the lockRecord parameter
        /// is true (in the case of GetItemExclusive), then GetSessionStoreItem
        /// locks the record and sets a new LockId and LockDate.
        /// </summary>
        private SessionStateStoreData GetSessionStoreItem(bool lockRecord, HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions) {
            // Initial values for return value and out parameters.
            lockAge = TimeSpan.Zero;
            lockId = null;
            locked = false;
            actions = 0;

            Logger.DebugFormat("Retrieving item from Cassandra. SessionId: {0}; ApplicationName: {1}.", id, ApplicationName);

            var table = _context.GetTable<UserSession>();
            var sessionState = table.FirstOrDefault(x => x.SessionId == id &&
                                                         x.ApplicationName == ApplicationName)
                                                         .Execute();

            if (sessionState == null) {
                Logger.DebugFormat("Item not found in Cassandra with SessionId: {0}; ApplicationName: {1}.", id, ApplicationName);
                return null;
            }

            //if the record is locked, we can't have it.
            if (sessionState.Locked) {
                Logger.DebugFormat("Item retrieved is locked. SessionId: {0}; ApplicationName: {1}.", id, ApplicationName);

                locked = true;
                lockAge = DateTime.UtcNow.Subtract((DateTime)sessionState.LockDate);
                lockId = sessionState.LockId;
                return null;
            }

            //generally we shouldn't get expired items, as the expiration bundle should clean them up,
            //but just in case the bundle isn't installed, or we made the window, we'll delete expired items here.
            if (sessionState.Expires < DateTime.UtcNow) {
                Logger.DebugFormat("Item retrieved has expired. SessionId: {0}; ApplicationName: {1}; Expiry (UTC): {2}",
                    id, ApplicationName, sessionState.Expires);

                table.Delete(sessionState);
                _context.SaveChanges(SaveChangesMode.Batch);

                return null;
            }

            if (lockRecord) {
                sessionState.Locked = true;
                sessionState.LockId += 1;
                sessionState.LockDate = DateTime.UtcNow;

                _context.SaveChanges(SaveChangesMode.Batch);
            }

            lockId = sessionState.LockId;
            return
                sessionState.Flags == (int)SessionStateActions.InitializeItem
                    ? new SessionStateStoreData(new SessionStateItemCollection(),
                                                SessionStateUtility.GetSessionStaticObjects(context),
                                                (int)_timeout.TotalMinutes)
                    : Deserialize(context, sessionState.SessionItems, (int)_timeout.TotalMinutes);
        }

        private string Serialize(SessionStateItemCollection items) {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream)) {
                if (items != null) {
                    items.Serialize(writer);
                }

                writer.Flush();
                writer.Close();

                return Convert.ToBase64String(stream.ToArray());
            }
        }

        private static SessionStateStoreData Deserialize(HttpContext context, string serializedItems, int timeout) {
            using (var stream = new MemoryStream(Convert.FromBase64String(serializedItems))) {
                var sessionItems = new SessionStateItemCollection();
                if (stream.Length > 0) {
                    using (var reader = new BinaryReader(stream)) {
                        sessionItems = SessionStateItemCollection.Deserialize(reader);
                    }
                }

                return new SessionStateStoreData(sessionItems,
                                                 SessionStateUtility.GetSessionStaticObjects(context),
                                                 timeout);
            }
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

    public class SessionStateContext : Context
    {
        public SessionStateContext(Session cqlSession)
            : base(cqlSession) {
            AddTable<UserSession>();
            CreateTablesIfNotExist();
        }
    }

    [AllowFiltering]
    [Table("sessions")]
    public class UserSession
    {
        public UserSession() {
            Created = DateTime.UtcNow;
            SessionItems = string.Empty;
        }

        public UserSession(string sessionId, string applicationName)
            : this() {
            SessionId = sessionId;
            ApplicationName = applicationName;
        }

        [PartitionKey]
        [Column("session_id")]
        public string SessionId { get; set; }

        [ClusteringKey(1),
        Column("application_name")]
        public string ApplicationName { get; set; }

        [Column("date_created")]
        public DateTime Created { get; set; }

        [Column("date_expires")]
        [SecondaryIndex]
        public DateTime Expires { get; set; }

        [Column("date_lock")]
        public DateTime? LockDate { get; set; }

        [Column("lock_id")]
        [SecondaryIndex]
        public int LockId { get; set; }

        [Column("is_locked")]
        public bool Locked { get; set; }

        [Column("items")]
        public string SessionItems { get; set; }

        [Column("flags")]
        public int Flags { get; set; }
    }
}
