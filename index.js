const mysql = require('mysql2'),
  mssql = require('mssql'),
  MongoClient = require('mongodb').MongoClient,
  { Pool } = require('pg'),
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
        "type": "string"
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
  }, // to test
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
  } // 目前仅支持 findOne 的操作，Finished
}

function DatabaseQuery(option, query) {
  // 校验参数
  let tv4res = tv4.validateResult(option, schema);

  return new Promise((resolve, reject) => {
    if (!tv4res?.valid) {
      reject({
        err: 'error',
        result: tv4res?.error?.message
      })
    }

    if (!_.isString(query)) {
      reject({
        err: 'error',
        result: 'Request parameter query must be a string'
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
          case 3: // 公钥
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

      const _DBExec = DBExec[option.type];

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

module.exports = DatabaseQuery;
module.exports.DatabaseQuery = DatabaseQuery;
