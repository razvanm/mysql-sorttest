This is a small program that tests the MySQL sort performance.

To install the MySQL driver using the [go tool](http://golang.org/cmd/go/ "go command") from shell:

```bash
$ go get github.com/go-sql-driver/mysql
```

To get this code:

```bash
$ go get github.com/razvanm/mysql-sorttest
```

The schema used by the test is the following:

```sql
CREATE TABLE `sorttest` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `data` char(120) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
```

How to create the table (1000 rows):

```bash
$ mysql-sorttest prepare
```

How to run a test (1 thread, 10 seconds):

```bash
$ mysql-sorttest run
```

How to delete the table:

```bash
$ mysql-sorttest run
```
