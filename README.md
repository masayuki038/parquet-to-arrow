parquet-to-arrow
================
parquet-to-arrow is a convertion library from Parquet files to Apache Arrow in Java.

Usage
-----

```java
VectorSchemaRoot vectorSchemaRoot = new ParquetToArrow().convert(your parquet file path);
```

Required Properties
-------------------

- `HADOOP_HOME` or `hadoop_home_dir`

Maven
-----

https://mvnrepository.com/artifact/net.wrap-trap/parquet-to-arrow
