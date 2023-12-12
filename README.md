<p align="center">
  <a href="https://adesign.apipost.cn/" target="_blank">
    <img alt="A-Design Logo" width="360" src="https://img.cdn.apipost.cn/cdn/opensource/apipost-opensource.svg" />
  </a>
</p>

# 🚀 apipost-grpc
Apipost 的 gRpc 模块，支持模拟 gRpc client 进行请求，并支持请求参数的Mock。

## Install

```
$ npm install apipost-grpc 
```

## 使用

```
const gRpcClient = new ApipostGrpc({
    proto:path.resolve(__dirname, './protos/HelloWorld.proto'),
    // ssl:{
    //     ca:path.resolve(__dirname, './cert/CA.crt'),
    //     key:path.resolve(__dirname, './cert/CLIENT_key.pem'),
    //     cert:path.resolve(__dirname, './cert/CLIENT.crt')
    // }
});
```

# request(service, method, target) 

**示例**：发送 gRpc 请求

```
let target = {
    "request": {
        "url": "127.0.0.1:50051",
        "header": [
            {
                "is_checked": "1",
                "key": "content-type",
                "value": "1111111"
            }
        ],
        "body": {
            "raw": `{"name": "jim", "city":"上海"}`
        }
    }
};
gRpcClient.request('Greeter', 'SayHello', target).then(function (response) {
    console.log(response)
})
```

# serverList() 

**示例**：获取所有 server 列表

```
gRpcClient.serverList()

```
返回：

```
[
    {
        "short": "Greeter", // 服务短名称
        "service": "helloworld.Greeter" // 服务长名称（带 namespac）
    }
]
```

# allMethodList() 

**示例**：获取所有 method 列表

```
gRpcClient.allMethodList()

```
返回：

```
{
    "helloworld.Greeter": { // 服务名（带 namespace）
        "service": "Greeter", // 服务名
        "method": [
            {
                "name": "SayHello", // 方法名
                "requestBody": { // 示例请求参数
                    "name": "本起层感去交",
                    "city": "性压件地经阶"
                }
            },
            {
                "name": "printAge",
                "requestBody": {
                    "age": "产年取产"
                }
            }
        ]
    }
}
```

# methodList(server) 

**示例**：获取指定server的method 列表

```
gRpcClient.methodList('helloworld.Greeter')

```
返回：

```
{
    "service": "Greeter", // 服务名
    "method": [
        {
            "name": "SayHello", // 方法名
            "requestBody": { // 示例请求参数
                "name": "果现不参应非",
                "city": "而而保"
            }
        },
        {
            "name": "printAge",
            "requestBody": {
                "age": "资着己南段"
            }
        }
    ]
}
```
