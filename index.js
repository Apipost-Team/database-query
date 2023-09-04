const mysql = require('mysql2'),
  mssql = require('mssql'),
  MongoClient = require('mongodb').MongoClient,
  { Pool } = require('pg'),
  { createClient } = require('redis'),
  db = require('dmdb'),
  oracledb = require('oracledb'),
  { ClickHouse } = require('clickhouse'),
  { Client } = require('ssh2'),
  _ = require('lodash'),
  tv4 = require('tv4'),
  fs = require('fs'),
  JSON5 = require('json5'),
  schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
      "type": {
        "type": "string",
        "enum": [
          "mysql",
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
    const connection = mysql.createConnection(_.assign(dbconfig, {
      port: _.isInteger(dbconfig.port) ? dbconfig.port : 3306
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
        connection.end();
      }
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
      return;
    });
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
      oracledb.initOracleClient({ poolTimeout: dbconfig.timeout });
      oracledb.getConnection(_.assign({
        user: dbconfig.user,
        password: dbconfig.password,
        connectString: `${dbconfig.host}:${dbconfig.port}/${dbconfig.database}`,
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
        result: `Oracle connect error: ${String(err.message)}`
      })
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    }
  }, // to test todo
  mongodb: function (dbconfig, resolve, reject, sshClient) {
    if (!_.isString(dbconfig.collectionName)) {
      reject({
        err: 'error',
        result: `Incorrect collectionName name`
      });
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    } else {
      MongoClient.connect(`mongodb://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 27017}/${dbconfig.database}`).then((client) => {
        client.db(dbconfig.database).collection(dbconfig.collectionName);

        resolve({
          err: 'success',
          result: `MongoDB connect success.`
        })

        _.isFunction(client.close) && client.close();
      }).catch((err) => {
        reject({
          err: 'error',
          result: `MongoDB connect error: ${String(err)}`
        })
      }).finally(() => {

        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
      });
    }
  }, // 目前仅支持 findOne 的操作，Finished to select method
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
          connection.end();

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
      oracledb.initOracleClient({ poolTimeout: dbconfig.timeout });
      oracledb.getConnection(_.assign({
        user: dbconfig.user,
        password: dbconfig.password,
        connectString: `${dbconfig.host}:${dbconfig.port}/${dbconfig.database}`,
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
        result: `Oracle connect error: ${String(err)}`
      })
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    }
  }, // to test todo
  mongodb: function (dbconfig, query, resolve, reject, sshClient) {
    if (!_.isString(dbconfig.collectionName)) {
      reject({
        err: 'error',
        result: `Incorrect collectionName name`
      });
      _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
    } else {
      MongoClient.connect(`mongodb://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 27017}/${dbconfig.database}`).then((client) => {
        const collection = client.db(dbconfig.database).collection(dbconfig.collectionName);
        // 查询文档
        try {
          if (query == '' || !_.isString(query)) {
            query = `{}`;
          }
          // collection.
          collection.findOne(JSON5.parse(query)).then((results) => {
            resolve({
              err: 'success',
              result: [JSON5.parse(JSON5.stringify(results))]
            })
          }).catch((err) => {
            reject({
              err: 'error',
              result: `MongoDB query error: ${String(err)}`
            })
          }).finally(() => {
            _.isFunction(client.close) && client.close();
            _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
          });
        } catch (e) {
          _.isFunction(client.close) && client.close();
          reject({
            err: 'error',
            result: `MongoDB query error: ${String(e)}`
          })
        }
      }).catch((err) => {
        reject({
          err: 'error',
          result: `MongoDB connect error: ${String(err)}`
        })
      }).finally(() => {
        _.isObject(sshClient) && _.isFunction(sshClient.end) && sshClient.end();
      });
    }
  }, // 目前仅支持 findOne 的操作，Finished to select method
  redis: async function (dbconfig, query, resolve, reject, sshClient) {
    const client = createClient({
      url: `redis://${dbconfig.user}:${dbconfig.password}@${dbconfig.host}:${dbconfig.port > 0 ? dbconfig.port : 6380}/${dbconfig.database ? dbconfig.database : 1}`
    });

    try {
      await client.connect();
      const results = await client[query?.method](...Object.values(query?.para));
      resolve({
        err: 'success',
        result: results
      })
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

    if (!_.isString(query) && ['mysql', 'mssql', 'pg', 'clickhouse', 'oracle', 'mongodb', 'dmdb'].indexOf(option.type) > -1) {
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

      const _DBExec = await DBConnectTest[option.type];

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