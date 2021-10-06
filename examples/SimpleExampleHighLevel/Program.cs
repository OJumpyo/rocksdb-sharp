using RocksDbSharp;
using System;
using System.Diagnostics;
using System.Text;
using System.IO;
using System.Linq;
using static RocksDbSharp.ColumnFamilies;
using System.Runtime.InteropServices;

namespace SimpleExampleHighLevel
{
    class Program
    {
        static void Main(string[] args)
        {

            string temp = Path.GetTempPath();
            string path = Environment.ExpandEnvironmentVariables(Path.Combine(temp, "rocksdb_simple_hl_example"));
            // the Options class contains a set of configurable DB options
            // that determines the behavior of a database
            // Why is the syntax, SetXXX(), not very C#-like? See Options for an explanation
            var options = new DbOptions()
                .SetCreateIfMissing(true)
                .EnableStatistics()
                .SetCreateMissingColumnFamilies()
                ;
            
            ColumnFamilyOptions cfo1 = new ColumnFamilyOptions();
            cfo1.SetNumLevels(4)
                .SetLevel0FileNumCompactionTrigger(2)
                .SetLevelCompactionDynamicLevelBytes(false)
                .SetMaxBytesForLevelBase(20480)
                .SetMaxBytesForLevelMultiplier(2.0)
                .SetTargetFileSizeBase(10240)
                .SetTargetFileSizeMultiplier(2)
                .SetCompactionStyle(Compaction.Level)
                ;
            ColumnFamilyOptions cfo2 = new ColumnFamilyOptions();
            cfo2.SetNumLevels(4)
                .SetLevel0FileNumCompactionTrigger(2)
                .SetLevelCompactionDynamicLevelBytes(false)
                .SetMaxBytesForLevelBase(20480)
                .SetMaxBytesForLevelMultiplier(2.0)
                .SetTargetFileSizeBase(10240)
                .SetTargetFileSizeMultiplier(2)
                .SetCompactionStyle(Compaction.Level)
                ;

            ColumnFamilies cfs = new ColumnFamilies(cfo1);

            //Descriptor descriptor1 = new Descriptor("default", cfo1);
            Descriptor descriptor2 = new Descriptor("raw", cfo2);
            //cfs.Add(descriptor1);
            cfs.Add(descriptor2);

            int ttls1 = 5 * 60 * 60;
            int ttls2 = 5 * 60 * 60;
            int[] ttlSeconds = new int[2];
            ttlSeconds[0] = ttls1;
            ttlSeconds[1] = ttls2;
            IntPtr ttls = IntArrayToIntPtr(ttlSeconds);
            
            {
                using (var db = RocksDb.OpenWithTtl(options, path, ttls, cfs))
                {
                    //
                    //RocksDb db = RocksDb.Open(options, path, cfs)

                    //putStringData(db, null);
                    //putStringData(db, db.GetColumnFamily("raw"));

                    //db.CreateColumnFamilyWithTtl(cfo2, "raw", 500000);
                    //db.CreateColumnFamily(cfo2, "raw");

                    string val = db.Get("k-1");
                    if (val == null) Console.WriteLine("null");
                    else Console.WriteLine(val);

                    TestLiveFiles(db);
                }
                    
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        internal static IntPtr IntArrayToIntPtr(int[] source)
        {
            if (source == null) return IntPtr.Zero;
            int[] da = source;
            IntPtr ptr = Marshal.AllocHGlobal(da.Length);
            Marshal.Copy(da, 0, ptr, da.Length);
            return ptr;
        }

        static void putStringData(RocksDb db, ColumnFamilyHandle cfh)
        {
            try
            {
                if (cfh == null)
                {

                    for (int i = 0; i < 1_000; i++)
                    {
                        Random random = new Random(i);
                        String key = "k-" + random.Next();
                        String value = "v-" + random.Next();
                        db.Put(key, value);
                    }
                }
                else
                {
                    for (int i = 0; i < 1_000; i++)
                    {
                        Random random = new Random(i);
                        String key = "k-" + i;
                        String value = "v-" + random.Next();
                        //String key = "k-" + i;
                        //String value = "v-" + i;
                        db.Put(key, value, cfh);
                    }
                }

                Console.WriteLine("putStringData success.");
            }
            catch (RocksDbException e)
            {
                Console.WriteLine(e.StackTrace);
            }

        }

        public static void TestLiveFiles(RocksDb db)
        {
            var dbName = "TestLiveFiles";
            DeleteDb(dbName);

            string temp = Path.GetTempPath();
            string path = Environment.ExpandEnvironmentVariables(Path.Combine(temp, "rocksdb_simple_hl_example"));
            var options = new DbOptions().SetCreateIfMissing(true);
            var flushOptions = new FlushOptions().SetWaitForFlush(true);

            {
                //using (var db = RocksDb.Open(options, path))
                {
                    var files = db.GetLiveFilesMetadata();

                    foreach (LiveFileMetadata metadata in files)
                    {
                        Console.WriteLine("level: " + metadata.FileMetadata.FileLevel + " fileName: " + metadata.FileMetadata.FileName + " fileSize " + metadata.FileMetadata.FileSize);
                        Console.WriteLine(metadata.FileDataMetadata.NumEntriesInFile);
                    }
                }
            }

            {
                //using (var db = RocksDb.Open(options, dbName))
                //{
                //    db.Put("key0", "value0");
                //    db.Put("key1", "value0");
                //    db.Flush(flushOptions);

                //    db.Put("key7", "value0");
                //    db.Put("key8", "value0");

                //    db.Flush(flushOptions);

                //    var files = db.GetLiveFilesMetadata();
                //    var fileNames = files.Select(file => file.FileMetadata.FileName);
                //    var fileList = Directory.EnumerateFiles(dbName);

                //    Debug.Assert(fileList.All(file => fileList.Contains(file)));
                //    Debug.Equals(db.Get("key0"), "value0");
                //}
            }
        }

        //[Fact]
        public void TestLiveFileNames()
        {
            var dbName = "TestLiveFiles";
            DeleteDb(dbName);
            var options = new DbOptions().SetCreateIfMissing(true);
            var flushOptions = new FlushOptions().SetWaitForFlush(true);

            using (var db = RocksDb.Open(options, dbName))
            {
                db.Put("key0", "value0");
                db.Put("key1", "value0");
                db.Flush(flushOptions);

                db.Put("key7", "value0");
                db.Put("key8", "value0");

                db.Flush(flushOptions);

                var files = db.GetLiveFileNames();
                var fileList = Directory.EnumerateFiles(dbName);

                Debug.Assert(fileList.All(file => fileList.Contains(file)));
                Debug.Equals(db.Get("key0"), "value0");
            }
        }

        public static void DeleteDb(string dbName)
        {
            if (Directory.Exists(dbName))
            {
                Directory.Delete(dbName, true);
            }
        }

        //static void Main(String[] args)
        //{
        //    string temp = Path.GetTempPath();
        //    string path = Environment.ExpandEnvironmentVariables(Path.Combine(temp, "rocksdb_simple_hl_example"));
        //    // the Options class contains a set of configurable DB options
        //    // that determines the behavior of a database
        //    // Why is the syntax, SetXXX(), not very C#-like? See Options for an explanation
        //    var options = new DbOptions()
        //        .SetCreateIfMissing(true)
        //        .EnableStatistics();
        //    using (var db = RocksDb.Open(options, path))
        //    {
        //        try
        //        {
        //            {
        //                // With strings
        //                string value = db.Get("key");
        //                db.Put("key", "value");
        //                value = db.Get("key");
        //                string iWillBeNull = db.Get("non-existent-key");
        //                db.Remove("key");
        //            }

        //            {
        //                // With bytes
        //                var key = Encoding.UTF8.GetBytes("key");
        //                byte[] value = Encoding.UTF8.GetBytes("value");
        //                db.Put(key, value);
        //                value = db.Get(key);
        //                byte[] iWillBeNull = db.Get(new byte[] { 0, 1, 2 });
        //                db.Remove(key);

        //                db.Put(key, new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 });
        //            }

        //            {
        //                // With buffers
        //                var key = Encoding.UTF8.GetBytes("key");
        //                var buffer = new byte[100];
        //                long length = db.Get(key, buffer, 0, buffer.Length);
        //            }

        //            {
        //                // Removal of non-existent keys
        //                db.Remove("I don't exist");
        //            }

        //            {
        //                // Write batches
        //                // With strings
        //                using (WriteBatch batch = new WriteBatch()
        //                    .Put("one", "uno")
        //                    .Put("two", "deuce")
        //                    .Put("two", "dos")
        //                    .Put("three", "tres"))
        //                {
        //                    db.Write(batch);
        //                }

        //                // With bytes
        //                var utf8 = Encoding.UTF8;
        //                using (WriteBatch batch = new WriteBatch()
        //                    .Put(utf8.GetBytes("four"), new byte[] { 4, 4, 4 })
        //                    .Put(utf8.GetBytes("five"), new byte[] { 5, 5, 5 }))
        //                {
        //                    db.Write(batch);
        //                }
        //            }

        //            {
        //                // Snapshots
        //                using (var snapshot = db.CreateSnapshot())
        //                {
        //                    var before = db.Get("one");
        //                    db.Put("one", "1");

        //                    var useSnapshot = new ReadOptions()
        //                        .SetSnapshot(snapshot);

        //                    // the database value was written
        //                    Debug.Assert(db.Get("one") == "1");
        //                    // but the snapshot still sees the old version
        //                    var after = db.Get("one", readOptions: useSnapshot);
        //                    Debug.Assert(after == before);
        //                }
        //            }

        //            var two = db.Get("two");
        //            Debug.Assert(two == "dos");

        //            {
        //                // Iterators
        //                using (var iterator = db.NewIterator(
        //                    readOptions: new ReadOptions()
        //                        .SetIterateUpperBound("t")
        //                        ))
        //                {
        //                    iterator.Seek("k");
        //                    Debug.Assert(iterator.Valid());
        //                    Debug.Assert(iterator.StringKey() == "key");
        //                    iterator.Next();
        //                    Debug.Assert(iterator.Valid());
        //                    Debug.Assert(iterator.StringKey() == "one");
        //                    Debug.Assert(iterator.StringValue() == "1");
        //                    iterator.Next();
        //                    Debug.Assert(!iterator.Valid());
        //                }
        //            }

        //        }
        //        catch (RocksDbException)
        //        {

        //        }
        //    }
        //}
    }
}
