const mysql = require('mysql2'),
  mssql = require('mssql'),
  // MongoClient = require('mongodb').MongoClient,
  { Pool } = require('pg'),
  { createClient } = require('redis'),
  oracledb = require('oracledb'),
  { ClickHouse } = require('clickhouse'),
  { Client } = require('ssh2'),
  _ = require('lodash'),
  tv4 = require('tv4'),
  fs = require('fs'),
  JSON5 = require('json5'),
  path = require('path'),
  schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
      "type": {
        "type": "string",
        "enum": [
          "mysql",
          "goldendb",
          "mssql",
          "oracle",
          "redis",
          "clickhouse",
          "dmdb",
          "mongodb",
          "pg"
        ]
      },
      "dbconfig": {
        "type": "object",
        "properties": {
          "host": {
            "type": "string"
          },
          "port": {
            "type": "integer"
          },
          "user": {
            "type": "string"
          },
          "password": {
            "type": "string"
          },
          "database": {
            "type": "string"
          },
          "collectionName": {
            "type": "string"
          }
        },
        "required": [
          "host",
          "user",
          "password",
          "database"
        ]
      },
      "ssh": {
        "type": "object",
        "properties": {
          "host": {
            "type": "string"
          },
          "port": {
            "type": "integer"
          },
          "username": {
            "type": "string"
          },
          "password": {
            "type": "string"
          },
          "privateKey": {
            "type": "string"
          },
          "passphrase": {
            "type": "string"
          }
        },
        "required": [
          "host",
          "port",
          "username"
        ]
      }
    },
    "required": [
      "type",
      "dbconfig",
      "ssh"
    ]
  }

// 各种数据库测试连接对象
const DBConnectTest = {
  mysql: function (dbconfig, resolve, reject, sshClient) {
    try {
      const connection = mysql.createConnection(_.assign(dbconfig, {
        port: _.isInteger(dbconfig.port) ? Number(dbconfig.port) : 3306
      }));

      connection.connect((err) => {
        if (err) {
          reject({
            err: 'error',
            result: `MySQL connection error: ${String(err)}`
          })
        } else {
          resolve({
            err: 'success',
            result: 'MySQL connection success.'
          })
        }

        if (_.isFunction(connection.end)) {
          try {
            connection.end();
          } catch (e) { }
        }
        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
        return;
      });
    } catch (err) {
      reject({
        err: 'error',
        result: `MySQL connection error: ${String(err)}`
      })
    }
  }, // Finished
  goldendb: function (dbconfig, resolve, reject, sshClient) {
    try {
      const connection = mysql.createConnection(_.assign(dbconfig, {
        port: _.isInteger(dbconfig.port) ? Number(dbconfig.port) : 3306
      }));

      connection.connect((err) => {
        if (err) {
          reject({
            err: 'error',
            result: `GoldenDB connection error: ${String(err)}`
          })
        } else {
          resolve({
            err: 'success',
            result: 'GoldenDB connection success.'
          })
        }

        if (_.isFunction(connection.end)) {
          try {
            connection.end();
          } catch (e) { }
        }
        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
        return;
      });
    } catch (err) {
      reject({
        err: 'error',
        result: `GoldenDB connection error: ${String(err)}`
      })
    }
  }, // Finished
  mssql: async function (dbconfig, resolve, reject, sshClient) {
    _.assign(dbconfig, {
      server: dbconfig.host,
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 1433,
      options: {
        encrypt: true,
        trustServerCertificate: true,
        cryptoCredentialsDetails: {},
        connectTimeout: dbconfig.timeout,
        requestTimeout: dbconfig.timeout
      }
    });

    try {
      await mssql.connect(dbconfig);
      resolve({
        err: 'success',
        result: 'Mssql connection success.'
      })
      mssql.close();
    } catch (err) {
      reject({
        err: 'error',
        result: `Mssql connection error: ${String(err)}`
      })
    } finally {
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    }
  }, // Finished
  pg: async function (dbconfig, resolve, reject, sshClient) {  // PostgreSQL
    // 创建 PostgreSQL 连接池配置
    const pool = new Pool(_.assign(dbconfig, {
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 5432
    }));

    try {
      const client = await pool.connect();
      resolve({
        err: 'success',
        result: 'PostgreSQL connection success.'
      })
      client.release();
    } catch (error) {
      reject({
        err: 'error',
        result: `PostgreSQL connection error: ${String(err)}`
      })
    } finally {
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
      pool.end();
    }
  }, // Finished
  clickhouse: function (dbconfig, resolve, reject, sshClient) {
    const clickhouse = new ClickHouse({
      url: dbconfig.host,
      port: dbconfig.port > 0 ? dbconfig.port : 8123,
      debug: false,
      basicAuth: {
        username: dbconfig.user,
        password: dbconfig.password
      },
      isUseGzip: false,
      trimQuery: false,
      usePost: false,
      format: "json", // "json" || "csv" || "tsv"
      raw: false,
      config: {
        database: dbconfig.database ? dbconfig.database : "default",
      }
    });
    clickhouse.query('SELECT 1').exec((err, rows) => {
      _.isFunction(clickhouse.close) && clickhouse.close();
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();

      if (err) {
        reject({
          err: 'error',
          result: `ClickHouse connection error: ${String(err)}`
        })
      } else {
        resolve({
          err: 'success',
          result: 'ClickHouse connection success.'
        })
      }
    });
  }, // Finished
  oracle: function (dbconfig, resolve, reject, sshClient) {
    try {
      dbconfig.port = _.isInteger(dbconfig.port) ? dbconfig.port : 1521;

      let libOpt = {};
      switch (process.platform) {
        case 'win32':
          libOpt = { oracleClientMode: oracledb.ORACLE_CLIENT_THIN }
          break;
        case 'darwin':
          libOpt = { libDir: '/usr/local/lib', oracleClientMode: oracledb.ORACLE_CLIENT_THIN }
        case 'linux':
          libOpt = { libDir: '/usr/local/lib', oracleClientMode: oracledb.ORACLE_CLIENT_THIN }
          break;
      }

      oracledb.initOracleClient(libOpt); // 设置 Oracle Instant Client 的路径

      oracledb.getConnection(_.assign({
        user: dbconfig.user,
        password: dbconfig.password,
        connectString: `${dbconfig.host}:${dbconfig.port}/${dbconfig.database ? dbconfig.database : dbconfig.server}`,
        driver: 'thin',
        sslVerifyCertificate: false
      }), (err, connection) => {
        if (err) {
          _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
          reject({
            err: 'error',
            result: `Oracle connect error: ${String(err)}`
          })
        } else {
          resolve({
            err: 'success',
            result: 'Oracle connect success.'
          })

          // 释放连接
          connection.close((err) => {
            if (err) {
              reject({
                err: 'error',
                result: `Oracle close error: ${String(err)}`
              })
            }
          });

          _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
        }
      })
    } catch (err) {
      reject({
        err: 'error',
        result: `Oracle connect error: ${String(err).split("\n")[0]}`
      })
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    }
  }, // to test todo
  // mongodb: function (dbconfig, resolve, reject, sshClient) {
  //   MongoClient.connect(`mongodb://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 27017}/${dbconfig.auth_source ? dbconfig.auth_source : dbconfig.database}`).then((client) => {

  //     resolve({
  //       err: 'success',
  //       result: `MongoDB connect success.`
  //     })

  //     _.isFunction(client.close) && client.close();
  //   }).catch((err) => {
  //     reject({
  //       err: 'error',
  //       result: `MongoDB connect error: ${String(err)}`
  //     })
  //   }).finally(() => {

  //     _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
  //   });
  // }, // 目前仅支持 findOne 的操作，Finished to select method
  redis: async function (dbconfig, resolve, reject, sshClient) {
    const client = createClient({
      url: `redis://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 6380}/${dbconfig.database ? dbconfig.database : 1}`
    });

    try {
      await client.connect();
      resolve({
        err: 'success',
        result: `Redis connect success.`
      })
    } catch (err) {
      reject({
        err: 'error',
        result: `Redis connect error:  ${String(err)}`
      })
    }

    if (_.isFunction(client.disconnect)) {
      await client.disconnect();
    }

    _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
  }, // Finished
  dmdb: async function (dbconfig, resolve, reject, sshClient) {
    let pool, conn;
    try {
      pool = await createPool();
      conn = await getConnection();
      resolve({
        err: 'success',
        result: 'Dmdb connect success.'
      })
    } catch (err) {
      reject({
        err: 'error',
        result: `Dmdb connect error: ${String(err)}`
      })
    } finally {
      try {
        await conn.close();
        await pool.close();
      } catch (err) {
        reject({
          err: 'error',
          result: `Dmdb close error: ${String(err)}`
        })
      }
    }

    _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();

    /* 创建连接池 */
    async function createPool() {
      try {
        let db = require('dmdb');

        return db.createPool({
          connectString: `dm://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 5236}?loginEncrypt=false&autoCommit=false`,
          poolMax: 10,
          poolMin: 1
        });
      } catch (err) {
        throw new Error("createPool error: " + err.message);
      }
    }

    /* 获取数据库连接 */
    async function getConnection() {
      try {
        return pool.getConnection();
      } catch (err) {
        throw new Error("getConnection error: " + err.message);
      }
    }
  } // Finished
}

// 各种数据库实际读取对象
const DBExec = {
  mysql: function (dbconfig, query, resolve, reject, sshClient) {
    const connection = mysql.createConnection(_.assign(dbconfig, {
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 3306
    }));

    connection.on('error', (err) => {
      reject({
        err: 'error',
        result: `MySQL connection error: ${String(err)}`
      })
    });

    connection.connect((err) => {
      if (err) {
        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
        reject({
          err: 'error',
          result: `MySQL connection error: ${String(err)}`
        })
        return;
      }

      connection.execute(
        query,
        function (err, results) {
          _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();

          try {
            connection.end();
          } catch (e) { }

          if (err) {
            reject({
              err: 'error',
              result: `MySQL execute error: ${String(err)}`
            })
          } else {
            resolve({
              err: 'success',
              result: results
            })
          }
        }
      );
    });
  }, // Finished
  goldendb: function (dbconfig, query, resolve, reject, sshClient) {
    const connection = mysql.createConnection(_.assign(dbconfig, {
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 3306
    }));

    connection.on('error', (err) => {
      reject({
        err: 'error',
        result: `MySQL connection error: ${String(err)}`
      })
    });

    connection.connect((err) => {
      if (err) {
        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
        reject({
          err: 'error',
          result: `GoldenDB connection error: ${String(err)}`
        })
        return;
      }

      connection.execute(
        query,
        function (err, results) {
          _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();

          try {
            connection.end();
          } catch (e) { }

          if (err) {
            reject({
              err: 'error',
              result: `GoldenDB execute error: ${String(err)}`
            })
          } else {
            resolve({
              err: 'success',
              result: results
            })
          }
        }
      );
    });
  }, // Finished 
  mssql: function (dbconfig, query, resolve, reject, sshClient) {
    _.assign(dbconfig, {
      server: dbconfig.host,
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 1433,
      options: {
        encrypt: true,
        trustServerCertificate: true,
        cryptoCredentialsDetails: {},
        connectTimeout: dbconfig.timeout,
        requestTimeout: dbconfig.timeout
      }
    });

    mssql.on('error', err => {
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
      reject({
        err: 'error',
        result: `Mssql connection error: ${String(err)}`
      })
    })

    mssql.connect(dbconfig).then(pool => {
      return pool.request()
        .query(query)
    }).then(results => {
      resolve({
        err: 'success',
        result: _.isArray(results.recordset) ? results.recordset : []
      })
    }).catch(err => {
      reject({
        err: 'error',
        result: `Mssql execute error: ${String(err)}`
      })
    }).finally(function () {
      mssql.close();
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    });
  }, // Finished
  pg: function (dbconfig, query, resolve, reject, sshClient) {  // PostgreSQL
    // 创建 PostgreSQL 连接池配置
    const pool = new Pool(_.assign(dbconfig, {
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 5432
    }));
    pool.query({
      text: query,
      timeout: dbconfig.timeout
    })
      .then(result => {
        resolve({
          err: 'success',
          result: _.isArray(result.rows) ? result.rows : []
        })
      })
      .catch(err => {
        reject({
          err: 'error',
          result: `PostgreSQL query error: ${String(err)}`
        })
      }).finally(() => {
        pool.end();
        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
      });
  }, // Finished
  clickhouse: function (dbconfig, query, resolve, reject, sshClient) {
    const clickhouse = new ClickHouse({
      url: dbconfig.host,
      port: dbconfig.port > 0 ? dbconfig.port : 8123,
      debug: false,
      basicAuth: {
        username: dbconfig.user,
        password: dbconfig.password
      },
      isUseGzip: false,
      trimQuery: false,
      usePost: false,
      format: "json", // "json" || "csv" || "tsv"
      raw: false,
      config: {
        database: dbconfig.database ? dbconfig.database : "default",
      }
    });

    clickhouse.query(query).exec(function (err, rows) {
      _.isFunction(clickhouse.close) && clickhouse.close();
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();

      if (err) {
        reject({
          err: 'error',
          result: `Clickhouse query error: ${String(err)}`
        })
      } else {
        resolve({
          err: 'success',
          result: rows ? rows : []
        })
      }
    });
  }, // Finished
  oracle: function (dbconfig, query, resolve, reject, sshClient) {
    try {
      dbconfig.port = _.isInteger(dbconfig.port) ? dbconfig.port : 1521;

      let libOpt = {};
      switch (process.platform) {
        case 'win32':
          libOpt = { oracleClientMode: oracledb.ORACLE_CLIENT_THIN }
          break;
        case 'darwin':
          libOpt = { libDir: '/usr/local/lib', oracleClientMode: oracledb.ORACLE_CLIENT_THIN }
        case 'linux':
          libOpt = { libDir: '/usr/local/lib', oracleClientMode: oracledb.ORACLE_CLIENT_THIN }
          break;
      }

      oracledb.initOracleClient(libOpt); // 设置 Oracle Instant Client 的路径

      oracledb.getConnection(_.assign({
        user: dbconfig.user,
        password: dbconfig.password,
        connectString: `${dbconfig.host}:${dbconfig.port}/${dbconfig.database ? dbconfig.database : dbconfig.server}`,
        sslVerifyCertificate: false
      }), (err, connection) => {
        if (err) {
          _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
          reject({
            err: 'error',
            result: `Oracle connect error: ${String(err)}`
          })
        } else {
          connection.execute(query, (err, results) => {
            if (err) {
              reject({
                err: 'error',
                result: `Oracle execute error: ${String(err)}`
              })
            } else {
              resolve({
                err: 'success',
                result: results
              })
            }
          });

          // 释放连接
          connection.close((err) => {
            if (err) {
              reject({
                err: 'error',
                result: `Oracle close error: ${String(err)}`
              })
            }
          });

          _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
        }
      })
    } catch (err) {
      reject({
        err: 'error',
        result: `Oracle connect error: ${String(err).split("\n")[0]}`
      })
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    }
  }, // to test todo
  // mongodb: function (dbconfig, query, resolve, reject, sshClient) {
  //   if (!_.isString(_.get(query, 'collectionName'))) {
  //     reject({
  //       err: 'error',
  //       result: `Incorrect collectionName name`
  //     });
  //     _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
  //   } else {
  //     let _collectionName = _.get(query, 'collectionName');
  //     let _method = _.get(query, 'method');
  //     let _data = _.get(query, 'data');

  //     MongoClient.connect(`mongodb://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 27017}/${dbconfig.auth_source ? dbconfig.auth_source : dbconfig.database}`).then((client) => {
  //       const collection = client.db(dbconfig.database).collection(_collectionName);

  //       if (_.isFunction(collection[_method])) {
  //         // 查询文档
  //         try {
  //           let _para_data = {};

  //           if (_.isString(_data)) {
  //             try {
  //               _para_data = JSON5.parse(_data);
  //             } catch (e) { }
  //           } else if (_.isObject(_data)) {
  //             _para_data = _data;
  //           }

  //           collection[_method](_para_data).then((results) => {
  //             resolve({
  //               err: 'success',
  //               result: [JSON5.parse(JSON5.stringify(results))]
  //             })
  //           }).catch((err) => {
  //             reject({
  //               err: 'error',
  //               result: `MongoDB query error: ${String(err)}`
  //             })
  //           });
  //         } catch (e) {
  //           reject({
  //             err: 'error',
  //             result: `MongoDB query error: ${String(e)}`
  //           })
  //         }
  //       } else {
  //         reject({
  //           err: 'error',
  //           result: `MongoDB connect error: ${_method} is not supported.`
  //         })
  //       }

  //       _.isFunction(client.close) && client.close();
  //     }).catch((err) => {
  //       reject({
  //         err: 'error',
  //         result: `MongoDB connect error: ${String(err)}`
  //       })
  //     }).finally(() => {
  //       _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
  //     });
  //   }
  // }, // todo
  redis: async function (dbconfig, query, resolve, reject, sshClient) {
    const client = createClient({
      url: `redis://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 6380}/${dbconfig.database ? dbconfig.database : 1}`
    });

    try {
      await client.connect();

      let data = [];

      if (_.isString(query?.data)) {
        try {
          data = JSON5.parse(query?.data);
        } catch (e) { }
      } else if (_.isObject(query?.data)) {
        data = query?.data;
      }

      if (_.isFunction(client[query?.method])) {
        const results = await client[query?.method](..._.values(data));
        resolve({
          err: 'success',
          result: results
        })
      } else {
        reject({
          err: 'error',
          result: `Redis query error ${query?.method} is not supported.`
        })
      }
    } catch (err) {
      reject({
        err: 'error',
        result: `Redis ${String(err)}`
      })
    }

    if (_.isFunction(client.disconnect)) {
      await client.disconnect();
    }

    _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
  }, // Finished
  dmdb: async function (dbconfig, query, resolve, reject, sshClient) {
    let pool, conn;
    try {
      pool = await createPool();
      conn = await getConnection();
      let results = await queryWithResultSet(query);
      resolve({
        err: 'success',
        result: results
      })
    } catch (err) {
      reject({
        err: 'error',
        result: `Dmdb ${String(err)}`
      })
    } finally {
      try {
        await conn.close();
        await pool.close();
      } catch (err) {
        reject({
          err: 'error',
          result: `Dmdb ${String(err)}`
        })
      }
    }

    _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();

    /* 创建连接池 */
    async function createPool() {
      try {
        let db = require('dmdb');

        return db.createPool({
          connectString: `dm://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 5236}?loginEncrypt=false&autoCommit=false`,
          poolMax: 10,
          poolMin: 1
        });
      } catch (err) {
        throw new Error("createPool error: " + err.message);
      }
    }
    /* 获取数据库连接 */
    async function getConnection() {
      try {
        return pool.getConnection();
      } catch (err) {
        throw new Error("getConnection error: " + err.message);
      }
    }
    /* 查询产品信息表 */
    async function queryWithResultSet(sql) {
      try {
        var result = await conn.execute(sql, [], { resultSet: true });
        var resultSet = result.resultSet;
        // 从结果集中获取一行
        let results = [];
        result = await resultSet.getRow();
        while (result) {
          results.push(result);
          result = await resultSet.getRow();
        }

        return results;
      } catch (err) {
        throw new Error("queryWithResultSet error: " + err.message);
      }
    }
  } // Finished
}

// 连接数据库并执行SQL等
function DatabaseQuery(option, query) {
  // 校验参数
  let tv4res = tv4.validateResult(option, schema);

  return new Promise(async (resolve, reject) => {
    if (!tv4res?.valid) {
      reject({
        err: 'error',
        result: tv4res?.error?.message
      })
    }

    if (!_.isString(query) && ['mysql', 'goldendb', 'mssql', 'pg', 'clickhouse', 'oracle', 'mongodb', 'dmdb'].indexOf(option.type) > -1) {
      reject({
        err: 'error',
        result: 'Request parameter query must be a string'
      })
    }

    if (!_.isObject(query) && ['redis'].indexOf(option.type) > -1) {
      reject({
        err: 'error',
        result: 'Request parameter query must be a Object'
      })
    }

    // ssh 配置项
    let sshConfig = {};

    if (option.ssh.enable > 0) {
      const ssh = option.ssh;
      try {
        switch (Number(ssh.enable)) {
          case 2: // 公钥
            _.assign(sshConfig, {
              host: ssh.host,
              port: ssh.port ? ssh.port : 22,
              username: ssh.username,
              privateKey: fs.readFileSync(ssh.privateKey)
            });
            break;
          case 3: // 公钥+密码
            _.assign(sshConfig, {
              host: ssh.host,
              port: ssh.port ? ssh.port : 22,
              username: ssh.username,
              privateKey: fs.readFileSync(ssh.privateKey),
              passphrase: ssh.passphrase
            });
            break;
          default: // 密码
            _.assign(sshConfig, {
              host: ssh.host,
              port: ssh.port ? ssh.port : 22,
              username: ssh.username,
              password: ssh.password
            });
            break;
        }
      } catch (e) {
        reject({
          err: 'error',
          result: `SSH Config error: ${String(e)}`
        })
      }
    }

    if (!_.isFunction(DBExec[option.type])) {
      reject({
        err: 'error',
        result: `Unsupported ${option.type} database operations.`
      })
    } else {
      const _dbconfig = _.cloneDeep(option.dbconfig, {
        port: Number(option.dbconfig.port),
        timeout: Number(option.dbconfig.timeout) >= 0 ? Number(option.dbconfig.timeout) : 10000
      });

      const _DBExec = await DBExec[option.type];

      if (option.ssh.enable > 0) {
        const sshClient = new Client();

        sshClient.on('ready', () => {
          sshClient.forwardOut(
            '127.0.0.1',
            0,
            option.dbconfig.host,
            option.dbconfig.port,

            (err, stream) => {
              if (err) {
                reject({
                  err: 'error',
                  result: `SSH forwardOut error: ${String(err)}`
                })
                return sshClient.end();
              } else {

                switch (option.type) {
                  case 'mysql':
                    _.assign(_dbconfig, {
                      stream: stream
                    })
                    break;
                  case 'goldendb':
                    _.assign(_dbconfig, {
                      stream: stream
                    })
                    break;
                  case 'mssql':
                    _.assign(_dbconfig, {
                      port: stream.localPort
                    })
                    break;
                  case 'redis':
                    _.assign(_dbconfig, {
                      socket: stream
                    })
                    break;
                }

                _DBExec(_dbconfig, query, resolve, reject, sshClient);
              }
            }
          );
        });

        sshClient.on('error', (err) => {
          reject({
            err: 'error',
            result: `SSH Client connect error: ${String(err)}`
          })
        });

        sshClient.connect(sshConfig);
      } else {
        _DBExec(_dbconfig, query, resolve, reject);
      }
    }
  })
}

// 检查数据库连接
function DatabaseConnectTest(option) {
  // 校验参数
  let tv4res = tv4.validateResult(option, schema);

  return new Promise(async (resolve, reject) => {
    if (!tv4res?.valid) {
      reject({
        err: 'error',
        result: tv4res?.error?.message
      })
    }

    // ssh 配置项
    let sshConfig = {};

    if (option.ssh.enable > 0) {
      const ssh = option.ssh;
      try {
        switch (Number(ssh.enable)) {
          case 2: // 公钥
            _.assign(sshConfig, {
              host: ssh.host,
              port: ssh.port ? ssh.port : 22,
              username: ssh.username,
              privateKey: fs.readFileSync(ssh.privateKey)
            });
            break;
          case 3: // 公钥+密码
            _.assign(sshConfig, {
              host: ssh.host,
              port: ssh.port ? ssh.port : 22,
              username: ssh.username,
              privateKey: fs.readFileSync(ssh.privateKey),
              passphrase: ssh.passphrase
            });
            break;
          default: // 密码
            _.assign(sshConfig, {
              host: ssh.host,
              port: ssh.port ? ssh.port : 22,
              username: ssh.username,
              password: ssh.password
            });
            break;
        }
      } catch (e) {
        reject({
          err: 'error',
          result: `SSH Config error: ${String(e)}`
        })
      }
    }

    if (!_.isFunction(DBExec[option.type])) {
      reject({
        err: 'error',
        result: `Unsupported ${option.type} database operations.`
      })
    } else {
      const _dbconfig = _.cloneDeep(option.dbconfig, {
        port: Number(option.dbconfig.port),
        timeout: Number(option.dbconfig.timeout) >= 0 ? Number(option.dbconfig.timeout) : 10000
      });

      const _DBExec = await DBConnectTest[option?.type];

      if (option.ssh.enable > 0) {
        const sshClient = new Client();

        sshClient.on('ready', () => {
          sshClient.forwardOut(
            '127.0.0.1',
            0,
            option.dbconfig.host,
            option.dbconfig.port,

            (err, stream) => {
              if (err) {
                reject({
                  err: 'error',
                  result: `SSH forwardOut error: ${String(err)}`
                })
                return sshClient.end();
              } else {

                switch (option.type) {
                  case 'mysql':
                    _.assign(_dbconfig, {
                      stream: stream
                    })
                    break;
                  case 'goldendb':
                    _.assign(_dbconfig, {
                      stream: stream
                    })
                    break;
                  case 'mssql':
                    _.assign(_dbconfig, {
                      port: stream.localPort
                    })
                    break;
                  case 'redis':
                    _.assign(_dbconfig, {
                      socket: stream
                    })
                    break;
                }

                _DBExec(_dbconfig, resolve, reject, sshClient);
              }
            }
          );
        });

        sshClient.on('error', (err) => {
          reject({
            err: 'error',
            result: `SSH Client connect error: ${String(err)}`
          })
        });

        sshClient.connect(sshConfig);
      } else {
        _DBExec(_dbconfig, resolve, reject);
      }
    }
  })
}
module.exports = DatabaseQuery;
module.exports.DatabaseQuery = DatabaseQuery;
module.exports.DatabaseConnectTest = DatabaseConnectTest;
