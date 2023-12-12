const grpc = require('@grpc/grpc-js'),
  path = require('path'),
  caller = require('grpc-caller'),
  protobuf = require("protobufjs"),
  fs = require('fs'),
  uuid = require('uuid'),
  Service = require("protobufjs/src/service"),
  Enum = require("protobufjs/src/enum"),
  Field = require("protobufjs/src/field"),
  MapField = require("protobufjs/src/mapfield"),
  Message = require("protobufjs/src/message"),
  OneOf = require("protobufjs/src/oneof"),
  Type = require("protobufjs/src/type"),
  protoLoader = require('@grpc/proto-loader'),
  Mock = require('mockjs'),
  _ = require('lodash'),
  JSON5 = require('json5'),
  ASideTools = require('apipost-inside-tools'),
  convertDescriptorToProto = require('filedescriptor2proto'),
  os = require('os'),
  grpc_reflection = require('grpc-reflection-js');

//127.0.0.1:50051
//59.110.10.84:30005

class GrpcClient {
  // 构造函数
  constructor(opts) {
    if (!opts) {
      opts = {};
    }

    // 初始化
    this.includeDirs = [];
    this.proto = opts.hasOwnProperty('proto') ? opts.proto : '';
    this.retry = opts.hasOwnProperty('retry') && _.isNumber(opts.retry) && opts.retry >= 0 ? opts.retry : 1; // fix bug
    this.ssl = opts.hasOwnProperty('ssl') ? opts.ssl : null;
    this.tls = opts.hasOwnProperty('tls') ? opts.tls : false;

    // 设置 import 依赖目录
    // 将当前proto文件所在的目录当作依赖目录之一
    const that = this;

    this.includeDirs.push(path.dirname(this.proto));

    if (_.isArray(opts.includeDirs)) {
      _.forEach(opts.includeDirs, function (dirpath) {
        if (_.isString(dirpath)) {
          that.includeDirs.push(dirpath);
        }
      });
    }

    if (this.proto != '') {
      try {
        this.packageDefinition = protoLoader.loadSync(
          this.proto,
          {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
            includeDirs: this.includeDirs
          }
        );
      } catch (e) {
        this.packageDefinition = {}
      }
    }

    // root
    this.root = new protobuf.Root();

    if (this.includeDirs.length > 0) {
      addIncludePathToRoot(this.root, this.includeDirs);
    }
  }

  /**
   * 发送
   */
  request(service, method, target, callback) {
    if (!_.isFunction(callback)) {
      callback = function () { }
    }

    const _grpc = this, _tls = this.tls;
    let _requestService = '';

    // get Service
    const serverList = this.serverList();

    if (_.isArray(serverList) && !_.isEmpty(serverList)) {
      this.serverList().forEach(item => {
        if (_.isObject(item) && service == item.service) {
          _requestService = item.short;
        }
      })
    } else {
      _requestService = String(service).substr(String(service).lastIndexOf(".") + 1);
    }

    // 请求 meta
    const meta = {};
    _.forEach(_.get(target, `request.header`), (item) => {
      if (item.is_checked > 0 && !_.isEmpty(_.trim(item.key))) {
        meta[_.trim(item.key)] = _.trim(item.value)
      }
    });

    // 请求body
    let body = {};
    try {
      body = JSON5.parse(String(_.get(target, `request.body.raw`)))
    } catch (e) { }

    // 请求 credentials
    let credentials = null;

    if (_.isObject(_grpc.ssl)) {
      credentials = grpc.credentials.createSsl(
        fs.readFileSync(_grpc.ssl.ca),
        fs.readFileSync(_grpc.ssl.key),
        fs.readFileSync(_grpc.ssl.cert)
      );
    } else {
      if (_tls > 0) {
        credentials = grpc.credentials.createSsl(); // fix bug
      }
    }

    // 请求开始时间
    const starttime = Date.now();

    // 开始请求
    const client = caller(_.get(target, `url`), {
      file: this.proto,
      load: {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
        includeDirs: this.includeDirs
      }
    }, _requestService, credentials);

    if (!_.isFunction(client[method?.name])) {
      callback(`Error: Service ${service} does NOT have a method named ${method?.name}.`, null)
    } else {
      if (method?.requestStream == -1 && method?.responseStream == -1) { // 一元调用
        client.Request(method?.name, body)
          .withMetadata(headers)
          .withResponseMetadata(true)
          .withResponseStatus(true)
          .withRetry(_grpc.retry)
          .exec(function (error, response) {
            let res = {
              "result": "",
              "metadata": [],
              "status": {
                "code": 0,
                "responseTime": (Date.now() - starttime),
                "details": "OK"
              }
            }

            if (_.isObject(response)) {
              let metadatas = [];
              _.forEach(_.get(response, `metadata.internalRepr`), function (values, key) {
                if (values instanceof Array) {
                  values.forEach(item => {
                    metadatas.push({
                      key: key,
                      value: item
                    });
                  })
                }
              });
              res.result = response.response
              res.metadata = metadatas;
              res.status.code = response.status.code;
              res.status.details = response.status.details;
              callback(null, res, null, `unary`)
            } else {
              callback(String(error), null, null, `unary`)
            }
          })
      } else {
        const callStream = client[method?.name](body, meta)

        if (method?.requestStream == -1 && method?.responseStream == 1) { // 服务端流式调用
          callback(null, null, callStream, `server_stream`)
        } else if (method?.requestStream == 1 && method?.responseStream == -1) { // 客户端流式调用
          callback(null, null, callStream, `client_stream`)
        } else if (method?.requestStream == 1 && method?.responseStream == 1) { // 双向流
          callback(null, null, callStream, `bidi_stream`)
        }
      }
    }
  }

  /**
     * 通过proto文件获取服务列表
     */
  serverList() {
    const services = [];
    try {
      const root = this.root.loadSync(this.proto, { keepCase: true });
      recursiveGetServer(root, services);
    } catch (e) { }
    return services;
  }

  /**
   *  通过proto文件，获取指定的service 方法列表
   * @param service
   * @returns {Array}
   */
  methodList(service) {
    const methods = {
      'service': service.substr(service.indexOf('.') + 1),
      'method': []
    };
    return new Promise((resove, reject) => {
      try {
        const listMethods = this.packageDefinition[service];
        const root = this.root.loadSync(this.proto, { keepCase: true });

        Object.keys(listMethods).forEach(method => {
          let fields = '{}';

          if (_.isObject(listMethods[method].requestType) && _.isObject(listMethods[method].requestType.type) && typeof listMethods[method].requestType.type.name === 'string') {
            fields = root.lookup(listMethods[method].requestType.type.name).fields
          }

          methods.method.push({
            name: method,
            requestStream: _.get(root.lookup(method), `requestStream`) ? 1 : -1,
            responseStream: _.get(root.lookup(method), `responseStream`) ? 1 : -1,
            requestBody: mockData(fields)
          })
        })
      } catch (e) {
        reject(String(e))
      }

      resove(methods);
    })
  }

  /**
   * 通过proto文件获取全部方法列表
   */
  allMethodList() {
    const methods = {};
    return new Promise((resove, reject) => {
      try {
        const root = this.root.loadSync(this.proto, { keepCase: true });

        this.serverList().forEach(item => {
          methods[item.service] = {
            'service': item.short,
            'method': []
          };
          const listMethods = this.packageDefinition[item.service];

          Object.keys(listMethods).forEach(method => {
            let fields = '{}';

            if (_.isObject(listMethods[method].requestType) && _.isObject(listMethods[method].requestType.type) && typeof listMethods[method].requestType.type.name === 'string') {
              fields = root.lookup(listMethods[method].requestType.type.name).fields

            }

            methods[item.service].method.push({
              name: method,
              requestStream: _.get(root.lookup(method), `requestStream`) ? 1 : -1,
              responseStream: _.get(root.lookup(method), `responseStream`) ? 1 : -1,
              requestBody: mockData(fields)
            })
          })
        })
      } catch (e) {
        reject(String(e))
      }

      resove(methods);
    })
  }

  /**
     * 通过反射获取全部方法列表
     */
  //   使用示例：
  //   allMethodListByReflection(`127.0.0.1:50051`,uuid.v4()).then((res) => {
  // 		console.log(JSON.stringify(res, null, "\t"))
  // 	}).catch((e)=>{
  // 		console.log(String(e))
  // 	})
  allMethodListByReflection(host, target_id = ``) {
    const allMethods = {};
    const reflection = new grpc_reflection.Client(host, grpc.credentials.createInsecure());


    return new Promise((resove, reject) => {
      try {
        reflection.listServices().then(async (services) => {
          const fs = require('fs'), uuid = require('uuid'), crypto = require('crypto'), path = require('path'), os = require('os'), tempDir = os.tmpdir();
          const protos = {
            includeDirs: [],
            includeFiles: [],
            protoContent: ``,
            protoPath: ``
          };

          target_id = String(target_id);

          if (_.isEmpty(target_id)) {
            target_id = uuid.v4();
          }

          for (let service of services) {
            const serviceInfo = await reflection.fileContainingSymbol(service);
            const dependency = [];
            _.forEach(Array.from(reflection?.fileDescriptorCache), function (item, key) {
              if (_.isArray(item[1]?.dependency)) {
                _.assign(dependency, item[1]?.dependency)
              }

              if (_.isObject(item[1]) && !_.isEmpty(item[1])) {
                const includeDirs = path.resolve(tempDir, `protos`, `${target_id}`, path.dirname(String(item[0])));
                const includeFiles = path.resolve(includeDirs, String(path.basename(item[0])));

                const protoContent = convertDescriptorToProto(JSON.parse(JSON.stringify(item[1])));

                try {
                  fs.mkdirSync(path.dirname(includeFiles), { recursive: true });
                  fs.writeFileSync(includeFiles, protoContent, 'utf8');

                  protos.includeDirs = _.union(protos.includeDirs, [includeDirs])
                  protos.includeFiles = _.union(protos.includeFiles, [{
                    name: path.basename(includeFiles),
                    path: includeFiles,
                    proto: protoContent,
                    relativePath: String(includeFiles).substring(String(tempDir).length + 1)
                  }])
                } catch (err) {
                  reject(err)
                }
              }
            });


            allMethods[service] = {
              service: service.substr(service.indexOf('.') + 1),
              method: []
            };

            _.forEach(_.get(serviceInfo.lookup(service), 'methods'), function (item, method) {
              allMethods[service].method.push({
                name: method,
                requestStream: serviceInfo.lookup(method)?.requestStream ? 1 : -1,
                responseStream: serviceInfo.lookup(method)?.responseStream ? 1 : -1,
                requestBody: mockData(serviceInfo.lookup(serviceInfo.lookup(method)?.requestType).fields)
              })
            });

            const mainProto = _.find(protos?.includeFiles, function (item) {
              const depProto = _.find(dependency, function (dep) {
                return _.endsWith(item?.relativePath, dep);
              });

              return !_.endsWith(item?.relativePath, depProto);
            });

            _.set(protos, `protoPath`, mainProto?.path)
            _.set(protos, `protoContent`, mainProto?.proto)
          };
          _.assign(allMethods, { protos })

          resove(allMethods)
        }).catch((e) => {
          reject(e)
        })
      } catch (e) {
        reject(e)
      }
    })
  }

  /**
     * 通过反射获取指定的service method 的mock数据
     * @param service
     * @returns {Array}
     */
  //   使用示例：
  //   mockMethodRequestByReflection(`127.0.0.1:50051`,`demo.MessageService`,`SayHelloStream`).then((res) => {
  // 		console.log(JSON.stringify(res, null, "\t"))
  // 	}).catch((e)=>{
  // 		console.log(String(e))
  // 	})
  mockMethodRequestByReflection(host, service, method) {
    const reflection = new grpc_reflection.Client(host, grpc.credentials.createInsecure());
    return new Promise(async (resove, reject) => {
      try {
        const serviceInfo = await reflection.fileContainingSymbol(service);
        resove(mockData(serviceInfo.lookup(serviceInfo.lookup(method)?.requestType).fields))
      } catch (e) {
        reject(String(e))
      }
    })
  }

  /**
   *  通过proto文件,获取指定的service method 的mock数据
   * @param service
   * @returns {Array}
   */
  mockMethodRequest(service, method) {
    return new Promise((resove, reject) => {
      try {
        let listMethods = this.packageDefinition[service];
        let root = this.root.loadSync(this.proto, { keepCase: true });

        let fields = '{}';

        if (_.isObject(listMethods) && _.isObject(listMethods[method]) && _.isObject(listMethods[method].requestType) && _.isObject(listMethods[method].requestType.type) && typeof listMethods[method].requestType.type.name === 'string') {
          fields = root.lookup(listMethods[method].requestType.type.name).fields
        }

        resove(mockData(fields));
      } catch (e) {
        reject(String(e))
      }
    })

  }
}


//	内部函数，根据字段生成mock对象
function mockData(obj) {
  obj = _.cloneDeep(obj);
  const MAX_STACK_SIZE = 3;

  function mockTypeFields(type, stackDepth = {}) {
    if (stackDepth[type.name] > MAX_STACK_SIZE) {
      return {};
    }
    if (!stackDepth[type.name]) {
      stackDepth[type.name] = 0;
    }

    stackDepth[type.name]++;

    const fieldsData = {};

    return type.fieldsArray.reduce((data, field) => {
      field.resolve();

      if (field.parent !== field.resolvedType) {
        if (field.repeated) {
          data[field.name] = [mockField(field, stackDepth)];
        } else {
          data[field.name] = mockField(field, stackDepth);
        }
      }

      return data;
    }, fieldsData);
  }

  function mockEnum(enumType) {
    const enumKey = Object.keys(enumType.values)[_.random(0, Object.keys(enumType.values).length - 1)];
    return enumType.values[enumKey];
  }

  function mockField(field, stackDepth = {}) {
    try {
      field.resolve();
    } catch (e) { }

    if (field instanceof MapField) {
      let mockPropertyValue = null;

      if (field.resolvedType === null) {
        mockPropertyValue = mockScalar(field.type, field.name);
      }

      if (mockPropertyValue === null) {
        try {
          const resolvedType = field.resolvedType;

          if (resolvedType instanceof Type) {
            if (resolvedType.oneofs) {
              mockPropertyValue = pickOneOf(resolvedType.oneofsArray);
            } else {
              mockPropertyValue = mockTypeFields(resolvedType);
            }
          } else if (resolvedType instanceof Enum) {
            mockPropertyValue = mockEnum(resolvedType);
          } else if (resolvedType === null) {
            mockPropertyValue = {};
          }
        } catch (e) { }
      }

      return {
        [mockScalar(field.keyType, field.name)]: mockPropertyValue,
      };
    }

    if (field.resolvedType instanceof Type) {
      return mockTypeFields(field.resolvedType, stackDepth);
    }

    if (field.resolvedType instanceof Enum) {
      return mockEnum(field.resolvedType);
    }

    const mockPropertyValue = mockScalar(field.type, field.name);

    if (mockPropertyValue === null) {
      const resolvedField = field.resolve();
      return mockField(resolvedField, stackDepth);
    } else {
      return mockPropertyValue;
    }
  }

  function pickOneOf(oneofs) {
    return oneofs.reduce((fields, oneOf) => {
      fields[oneOf.name] = mockField(oneOf.fieldsArray[0]);
      return fields;
    }, {});
  }

  function mockScalar(type, fieldName) {
    switch (type) {
      case 'string':
        return interpretMockViaFieldName(fieldName);
      case 'bool':
        return true;
      case 'number':
      case 'int32':
      case 'int64':
      case 'uint32':
      case 'uint64':
      case 'sint32':
      case 'sint64':
      case 'fixed32':
      case 'fixed64':
      case 'sfixed32':
      case 'sfixed64':
        return Mock.mock(`@integer(1,1000)`);
      case 'double':
      case 'float':
        return Mock.mock(`@float( 1, 10, 2, 5 )`);
      case 'bytes':
        return new Buffer('Hello Apipost');
      default:
        return null;
    }
  }

  function interpretMockViaFieldName(fieldName) {
    const fieldNameLower = fieldName.toLowerCase();

    if (fieldNameLower.startsWith('id') || fieldNameLower.endsWith('id')) {
      return uuid.v4();
    }

    return Mock.mock(`@ctitle()`);
  }

  if (_.isObject(obj)) {
    Object.keys(obj).forEach(key => {
      if (obj[key].parent !== obj[key].resolvedType) {
        if (obj[key].repeated) {
          let random = Math.random() * 1000 + 1;
          obj[key] = [mockField(obj[key]), parseInt(random)]
        } else {
          obj[key] = mockField(obj[key])
        }
      }
    })
  }

  return obj
}

// 内部函数，给 protobufjs 指定 import依赖
function addIncludePathToRoot(root, includePaths) {
  const originalResolvePath = root.resolvePath;
  root.resolvePath = (origin, target) => {
    if (path.isAbsolute(target)) {
      return target;
    }
    for (const directory of includePaths) {
      const fullPath = path.join(directory, target);
      try {
        fs.accessSync(fullPath, fs.constants.R_OK);
        return fullPath;
      } catch (err) {
        continue;
      }
    }
    return originalResolvePath(origin, target);
  };
}

// 内部函数，结果转换函数
function ConvertResult(status, message, data) {
  return ASideTools.ConvertResult(status, message, data)
}

// 内部函数，递归遍历获取server
function recursiveGetServer(root, services, nestedTypeName = '') {
  if (root instanceof protobuf.Root || root instanceof protobuf.Namespace) {
    Object.keys(root.nested || {}).forEach((key) => {
      const nestedType = root.lookup(key);
      if (nestedType instanceof protobuf.Service) {
        // 发现了一个服务
        services.push({
          short: key,
          service: nestedTypeName ? `${nestedTypeName}.${key}` : key
        });
      } else {
        // 递归查找更深层的服务
        recursiveGetServer(nestedType, services, nestedTypeName ? `${nestedTypeName}.${key}` : key);
      }
    });
  }
}

module.exports = GrpcClient;