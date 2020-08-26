# Quasar SAP HANA Plugin [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/pSSqJrr)

## Destination

The SAP HANA destination plugin enables Quasar to load data into SAP HANA, SAP Data Warehouse Cloud, or other protocol-compliant stores. Loading is done via `INSERT INTO` statements.

### Destination Configuration

JSON configuration required to construct a SAP HANA destination.

```
{
  "connection": <connection-configuration>,
  "writeMode": "create" | "replace" | "truncate" | "append"
}
```

* `connection`: A [connection configuration](#connection-configuration) object.
* `writeMode`: Indicates how to handle loading data into an existing table
  * `create`: prevent loading data into an existing table, erroring if it exists
  * `replace`: `DROP` and recreate an existing table prior to loading data
  * `truncate`: `TRUNCATE` an existing table prior to loading data
  * `append`: `APPEnd` to an existing table

## Connection Configuration

JSON configurating describing how to connect to SAP HANA.

```
{
  "jdbcUrl": String
  [, "maxConcurrency": Number]
  [, "maxLifetimeSecs": Number]
}
```

* `jdbcUrl`: a SAP HANA [connection string](https://help.sap.com/viewer/f1b440ded6144a54ada97ff95dac7adf/2.5/en-US/ff15928cf5594d78b841fbbe649f04b4.html). Note that any connection parameter values containing URI [reserved characters](https://tools.ietf.org/html/rfc3986#section-2.2) must be [percent encoded](https://tools.ietf.org/html/rfc3986#section-2.1) to avoid ambiguity.
* `maxConcurrency` (optional): the maximum number of simultaneous connections to the database (default: 8)
* `maxLifetimeSecs` (optional): the maximum lifetime, in seconds, of idle connections. If your database or infrastructure imposes any limit on idle connections, make sure to set this value to at most a few seconds less than the limit (default: 300 seconds)
