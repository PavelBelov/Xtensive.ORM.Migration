# Xtensive.ORM.Migration
##Migration tool for Xtensive.ORM

This tool can be used in case when you use Xtensive.ORM and want to migrate from one database to another.

Before migration process start you should create new database and built domain. The domain should be fully same as domain of your source database.

## Usages
```
Xtensive.Orm.Migration.exe srcProvider srcConnctionString dstProvider dstConnctionString [options]
  srcProvider          source provide (sqlserver, postgresql, ...)
  srcConnctionString   source connection string
  dstProvider          destination provide (sqlserver, postgresql, ...)
  dstConnctionString   destination connection string
Options:
--help                 show help
--force                do not ask for confirmation
```
