using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Web.SessionState;
using Cassandra.Data.Linq;
using NUnit.Framework;

namespace Cassandra.AspNet.SessionState.Tests
{
    [TestFixture]
    public class ConfigureClusterTests
    {
        private const string KeySpace = "SessionState_Test";

        private static Cluster CreateCluster() {
            var builder = Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .WithQueryTimeout(60 * 1000)
                .WithAsyncCallTimeout(360 * 1000)
                .WithCompression(CompressionType.Snappy)
                .WithoutRowSetBuffering();

            return builder.Build();
        }

        private static Session CreateSession(Cluster cluster) {
            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(KeySpace);
            session.ChangeKeyspace(KeySpace);

            return session;
        }

        [Test]
        public void CanCreateCluster() {
            var cluster = Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .Build();

            Assert.That(cluster, Is.Not.Null);
            Assert.That(cluster.Configuration.ClientOptions.WithoutRowSetBuffering, Is.True);
            Assert.That(cluster.Configuration.ProtocolOptions.Compression, Is.EqualTo(CompressionType.Snappy));
        }

        [Test]
        public void CanCreateKeyspace() {
            var cluster = CreateCluster();
            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(KeySpace);
            session.ChangeKeyspace(KeySpace);

            Assert.That(session, Is.Not.Null);
            Assert.That(session.Keyspace, Is.EqualTo(KeySpace));
        }

        [Test]
        public void CanCreateTables() {
            var cluster = CreateCluster();
            var session = CreateSession(cluster);

            var context = new SessionStateContext(session);
            context.SaveChanges(SaveChangesMode.Batch);

            Assert.That(context, Is.Not.Null);
            Assert.That(context.HasTable<UserSession>(), Is.True);

            // clean up
            DropTables(context, session);
        }

        private static void DropTables(SessionStateContext context, Session session) {
            if (context.HasTable<UserSession>()) {
                var table = context.GetTable<UserSession>();
                var tableName = table.GetTableName();

                session.Execute(string.Format("DROP TABLE {0};", tableName));
            }
        }

        [Test]
        public void CanInsertUserSession() {
            var cluster = CreateCluster();
            var session = CreateSession(cluster);

            // remove table at the start
            session.Execute("DROP TABLE sessions;");

            var context = new SessionStateContext(session);
            var table = context.GetTable<UserSession>();

            var countBefore = table.Count().Execute();

            var batch = session.CreateBatch();

            var userSession = new UserSession(Guid.NewGuid().ToString(), "/") {
                Expires = DateTime.UtcNow.AddMinutes(20),
                Flags = (int)SessionStateActions.InitializeItem,
                SessionItems = new object().GetHashCode().ToString(),
                Locked = false
            };

            batch.Append(table.Insert(userSession));
            batch.Execute();

            var countAfter = table.Count().Execute();
            Assert.That(countAfter, Is.GreaterThan(countBefore));
        }

        [Test]
        public void CanInsertLargeNumberOfUserSession() {
            var cluster = CreateCluster();
            var session = CreateSession(cluster);

            // remove table at the start
            session.Execute("DROP TABLE sessions;");

            var context = new SessionStateContext(session);

            var table = context.GetTable<UserSession>();

            var countBefore = table.Count().Execute();

            const int count = 10000;
            for (int i = 0; i < count; i++) {
                var userSession = new UserSession(Guid.NewGuid().ToString(), "/") {
                    Expires = DateTime.UtcNow.AddMinutes(20),
                    Flags = (int)SessionStateActions.InitializeItem,
                    SessionItems = new object().GetHashCode().ToString(),
                    Locked = false
                };

                table.AddNew(userSession, EntityTrackingMode.DetachAfterSave);
            }

            context.SaveChanges(SaveChangesMode.Batch);

            var countAfter = table.Count().Execute();
            Assert.That(countAfter, Is.GreaterThan(countBefore));
            Assert.That(countAfter, Is.EqualTo(count));
        }

        [Test]
        public void MassiveAsyncTest() {
            var cluster = CreateCluster();
            var session = CreateSession(cluster);

            var context = new SessionStateContext(session);
            // remove table at the start
            DropTables(context, session);

            var table = context.GetTable<UserSession>();
            table.CreateIfNotExists();

            const int totalRows = 100000;
            bool[] responses = new bool[totalRows];

            var thread = new Thread(() => {
                for (int i = 0; i < totalRows; i++) {
                    int tmpi = i;
                    Console.Write("+");
                    var batch = session.CreateBatch();
                    var userSession = new UserSession(Guid.NewGuid().ToString(), "/") {
                        Expires = DateTime.UtcNow.AddMinutes(20),
                        Flags = (int)SessionStateActions.InitializeItem,
                        SessionItems = new object().GetHashCode().ToString(),
                        Locked = false
                    };
                    batch.Append(table.Insert(userSession));
                    batch.BeginExecute((_) => {
                        responses[tmpi] = true;
                        Thread.MemoryBarrier();
                    }, null);
                }
            });

            thread.Start();

            HashSet<int> done = new HashSet<int>();
            while (done.Count < totalRows) {
                for (int i = 0; i < totalRows; i++) {
                    Thread.MemoryBarrier();
                    if (!done.Contains(i) && responses[i]) {
                        done.Add(i);
                        Console.Write("-");
                    }
                }
            }

            thread.Join();

            Console.WriteLine();
            Console.WriteLine("Inserted... now we are checking the count");

            var tableName = table.GetTableName();
            using (var ret = session.Execute(string.Format(@"SELECT * from {0} LIMIT {1};", tableName, totalRows + 100), ConsistencyLevel.Quorum)) {
                Assert.That(ret.GetRows().Count(), Is.EqualTo(totalRows));
            }
        }

        [Test]
        public void CanInsertAndQueryForSingleItem() {
            var cluster = CreateCluster();
            var session = CreateSession(cluster);

            // remove table at the start
            session.Execute("DROP TABLE sessions;");

            var context = new SessionStateContext(session);

            var table = context.GetTable<UserSession>();

            var uid = Guid.NewGuid().ToString();
            const string appName = "/";
            object lockId = 123;

            var userSession = new UserSession(uid, appName) {
                Expires = DateTime.UtcNow.AddMinutes(20),
                Flags = (int)SessionStateActions.InitializeItem,
                SessionItems = new object().GetHashCode().ToString(),
                Locked = true,
                LockId = (int)lockId,
                LockDate = DateTime.UtcNow,
            };

            table.AddNew(userSession, EntityTrackingMode.KeepAttachedAfterSave);

            context.SaveChanges(SaveChangesMode.OneByOne);

            var locatedSession = table.FirstOrDefault(x => x.SessionId == uid &&
                                                           x.ApplicationName == appName &&
                                                           x.LockId == (int)lockId).Execute();

            Assert.That(locatedSession, Is.Not.Null);
        }

        [Test]
        public void CanUpdateSingleItem() {
            var cluster = CreateCluster();
            var session = CreateSession(cluster);

            // remove table at the start
            session.Execute("DROP TABLE sessions;");

            var context = new SessionStateContext(session);

            var table = context.GetTable<UserSession>();

            var uid = Guid.NewGuid().ToString();
            const string appName = "/";
            object lockId = 123;

            var userSession = new UserSession(uid, appName) {
                Expires = DateTime.UtcNow.AddMinutes(20),
                Flags = (int)SessionStateActions.InitializeItem,
                SessionItems = new object().GetHashCode().ToString(),
                Locked = true,
                LockId = (int)lockId,
                LockDate = DateTime.UtcNow,
            };

            table.AddNew(userSession, EntityTrackingMode.DetachAfterSave);
            context.SaveChanges(SaveChangesMode.OneByOne);

            var t2 = context.GetTable<UserSession>();
            var sessionToUpdate = t2.FirstOrDefault(x => x.SessionId == uid &&
                                                           x.ApplicationName == appName &&
                                                           x.LockId == (int)lockId).Execute();
            t2.Attach(sessionToUpdate);

            sessionToUpdate.LockId += 1;
            context.SaveChanges(SaveChangesMode.OneByOne);

            // load again and verify the value has changed
            var t3 = context.GetTable<UserSession>();
            var loadedSession = t3.FirstOrDefault(x => x.SessionId == uid && x.ApplicationName == appName).Execute();

            Assert.That(loadedSession, Is.Not.Null);
            Assert.That(loadedSession.LockId, Is.EqualTo(124));
        }
    }
}
