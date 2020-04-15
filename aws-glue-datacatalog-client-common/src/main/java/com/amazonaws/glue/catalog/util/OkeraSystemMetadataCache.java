package com.amazonaws.glue.catalog.util;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.util.Map;
import java.util.Set;


 /**
 * This class implements get/get/invalidate helpers along with any general purpose TTL
 * requirements for caching glue metadata objects. While this uses Guava's
 * LoadingCache which is considered thread-safe, to remove any ambiguity, the class will
 * be implemented as a singleton.
 *
 * NOTE1:
 * We've standardized here on what Canonicalized Db and Tbl names are, and also
 * defined Fully Qualified Table Names. These entities are defined as follows:
 * - Canonicalized Db Name:- All lower case, whitespace trimmed, back-tick escape char
 *   removed Db name.
 *   eg. Input: "`Default `", Output: "default"
 * - Canonicalized Tbl Name: All lower case, whitespace trimmed, back-tick escape char
 *   removed Tbl name.
 *   eg. Input: "` okera_Sample`", Output: "okera_sample"
 * - Fully Qualified Tbl Name(FQTN): Concatenation of canonicalized db name and canonicalized
 *   tbl name.
 *
 * NOTE2:
 * We had two choics for the tblCache.
 * - Option 1 was <String, Map<table>> -> Keyed on Database Name.
 *   We did not pick this option since while the getIfPresent on the cache is thread-safe,
 *   calling a get on the map internally (even if we use a thread safe blocking Map impl)
 *   can affect atomicity of the overall get Operation if there is a read call when a
 *   write is being served. Given the targeted nature of this
 *   cache, doing this right via read/write locks is more hassle than worth, hence option
 *   2.
 *
 * - Option 2 was <String, Table> -> Keyed on Fully Qualified Table Name.
 *   While in this case, atomicity is lost between creating the FQTN and the getIfPresent
 *   operation from the cache, the object isn't susceptible to change between
 *   concurrent read and write. This does lead to O(n) worst case performance
 *   invalidating database
 *   (TODO: lock/synchronize the public methods?)
 */
public class OkeraSystemMetadataCache {
  private static final Logger logger = Logger.getLogger(OkeraSystemMetadataCache.class);

  private static OkeraSystemMetadataCache instance;

  // DB Caching constants
  private static final String OkeraSystemDbName = "okera_system";
  private static final String OkeraInternalDbPrefix = "_okera";
  private static final String OkeraCrawlerDbPrefix = "_okera_crawler";
  private static final char escapeChar = '`';

  // TODO: fancy pre-populating LoadingCache?
  private Cache<String,Table> tblCache = null;
  private Cache<String,Database> dbCache = null;

  private OkeraSystemMetadataCache()  {
    // TODO: expiry?
    // TODO: loadingCache (will need impl classes for table and db loaders)
    tblCache = CacheBuilder.newBuilder().build();
    dbCache = CacheBuilder.newBuilder().build();
  }

  public static OkeraSystemMetadataCache getInstance() {
    if (instance == null) {
      instance = new OkeraSystemMetadataCache();
    }
    return instance;
  }

  /**
   * Database get/set/invalidate for cached internal DBs
   */
  public void invalidateDb(String dbName) {
    dbName = getCanonicalDbName(dbName);
    dbCache.invalidate(dbName);
    // if a db is being invalidated, delete all table entries in the cache
    // for this db
    if (isCachedDb(dbName)) {
      // TODO: synchronize everything here. trash otherwise.
      // Does this still have a chance of hitting a ConcurrentModificationException?
      Set <String> tblNameSet = tblCache.asMap().keySet();
      for (String tblName : tblNameSet)  {
        if (tblName.contains(dbName)) {
          tblCache.invalidate(tblName);
        }
      }
    }
  }

  public void setDb(String dbName, Database dbObj) {
    dbName = getCanonicalDbName(dbName);
    if (isCachedDb(dbName))  {
      dbCache.put(dbName, dbObj);
    }
  }

  public Database getDb(String dbName)  {
    return dbCache.getIfPresent(getCanonicalDbName(dbName));
  }

  /**
   * Table get/set/invalidate for cached internal Tables
   */
  public void invalidateTable(String dbName, String tblName) {
    tblCache.invalidate(getFullyQualifiedTblName(dbName, tblName));
  }

  public void setTbl(String dbName, String tblName, Table tblObj) {
    if (isCachedDb(getCanonicalDbName(dbName))) {
      tblCache.put(getFullyQualifiedTblName(dbName, tblName), tblObj);
    }
  }

  public Table getTbl(String dbName, String tblName)  {
    return tblCache.getIfPresent(getFullyQualifiedTblName(dbName, tblName));
  }


  /**
   * Helpers
   */
  private String getFullyQualifiedTblName(String dbName, String tblName)  {
    return getCanonicalDbName(dbName).concat(getCanonicalTblName(tblName));
  }

  private String getCanonicalTblName(String tblName) {
    return tblName.toLowerCase().replace("`", "").trim();
  }

  private String getCanonicalDbName(String dbName)  {
    return dbName.toLowerCase().replace("`", "").trim();
  }

  private boolean isCachedDb(String name) {
    name = getCanonicalDbName(name);
    if (name.equalsIgnoreCase(DEFAULT_DATABASE_NAME)
        || name.equalsIgnoreCase(OkeraSystemDbName))  {
      return true;
    }
    if (name.toLowerCase().startsWith(OkeraCrawlerDbPrefix))  {
      return false;
    }
    if (name.toLowerCase().startsWith(OkeraInternalDbPrefix)) {
      return true;
    }
    return false;
  }

}