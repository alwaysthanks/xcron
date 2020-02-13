[TOC]

### 分布式任务调度系统

#### 1.问题现象

- 支付系统每天凌晨1点进行前一天清算，每月1号进行上个月清算
- 电商整点抢购，商品价格8点整开始优惠
- 12306购票系统，超过30分钟没有成功支付订单的，进行回收处理

#### 2.功能需求

- 执行指定时间点进行任务触发
- 支持周期性任务触发

#### 3.系统特点

- 高可用, 高可靠
- 数据强一致性
- 任务故障转移
- 集群快速扩容
- 机器指标监控

#### 4.系统设计

(1). 系统架构

<center>
    <img src="https://github.com/alwaysthanks/xcron/blob/master/doc/xcron_arch.png">
</center>

(2). 部署架构

<center>
    <img src="https://github.com/alwaysthanks/xcron/blob/master/doc/cluster_arch.png">
</center>




#### 5.对外接口(HTTP)

对外提供HTTP协议**content-type: application/json**相关接口:

##### 5.1 创建任务

- 接口：/xcron/createTask
- 调用方式：POST
- 请求参数：

| 字段     | 类型       | 必选 | 说明                                                         |
| -------- | ---------- | ---- | ------------------------------------------------------------ |
| type     | int        | 是   | 枚举，任务类型: 1: 周期性任务; 2:定时任务                    |
| format   | string     | 是   | 当type=1, 周期性执行, 格式见linux crontab<br />当type=2, 单次执行, 格式为unix时间戳, 单位: 秒 |
| callback | callback表 | 是   | 任务执行时回调信息                                           |

callback表:

| 字段 | 类型          | 必选 | 说明   |
| ----- | ------ | ---- | ---------- |
| url  | string        | 是   | 任务执行时回调的http地址, 以POST方式回调,content-type为application/json |
| body | map\<k\>v | 否   | 回调时，该参数值作为http回调的body内容                       |

- 返回参数：

| 字段    | 类型   | 说明                          |
| ------- | ------ | ----------------------------- |
| uuid    | string | 系统为该请求生成的唯一标识符  |
| code    | int    | code为0表示成功，其他表示失败 |
| message | string | code不为0时，表示异常信息     |
| data    | data表 | 响应数据,见下data表           |

data表:

| 字段    | 类型   | 说明         |
| ------- | ------ | ------------ |
| task_id | string | 该任务唯一id |


- 响应示例：

```json
{
    "uuid":"29e2f4b4-0ffd-4eaf-8185-f55b45cc6f87",
    "code":0,
    "data":{
        "task_id":"e2f4b4-0ffd-4eaf-8185-f"
    }
}
```
##### 5.2 删除任务

- 接口：/xcron/deleteTask
- 调用方式：POST
- 请求参数：

|  字段   | 类型   | 必选 | 说明       |
| ----- | ------ | ---- | ---------- |
| task_id | string | 是   | 任务唯一id |

- 返回参数：

| 字段 | 类型   | 说明                          |
| ---- | ------ | ----------------------------- |
| uuid | string | 该请求生成的唯一标识符        |
| code | int    | code为0表示成功，其他表示失败 |

- 响应示例：

```json
{
	"uuid": "29e2f4b4-0ffd-4eaf-8185-f55b45cc6f87",
	"code": 0,
}
```
##### 5.3 查询任务

- 接口：/xcron/queryTask
- 调用方式：GET
- 请求参数：

|  字段   | 类型   | 必选 | 说明       |
| ----- | ------ | ---- | ---------- |
| task_id | string | 是   | 任务唯一id |

- 返回参数：

| 字段    | 类型   | 说明                          |
| ------- | ------ | ----------------------------- |
| uuid    | string | 该请求生成的唯一标识符        |
| code    | int    | code为0表示成功，其他表示失败 |
| message | string | code不为0时，表示异常信息     |
| data    | data表 | 响应数据，见下data表          |

data表:

| 字段        | 类型       | 说明           |
| ----------- | ---------- | -------------- |
| task_id     | string     | 任务唯一id     |
| create_time | string     | 任务创建时间戳 |
| type        | int        | 任务类型       |
| format      | string     | 任务格式       |
| run_count   | int        | 任务执行次数   |
| callback    | callback表 | 回调信息       |

callback表:

| 字段 | 类型          | 必选 | 说明                                      |
| -- | ------------- | ---- | ----------------------------------------|
| url  | string        | 是   | 任务执行时回调的http地址, 以POST方式回调,content-type为application/json |
| body | map\<key\>val | 否   | 回调时，该参数值作为http回调的body内容                       |


- 响应示例：

```json
{
    "uuid":"29e2f4b4-0ffd-4eaf-8185-f55b45cc6f87",
    "code":0,
    "data":{
        "task_id":"12345"
    }
}
```

#### 6.使用说明

```shell
#项目根目录
bash build.sh
#修改配置xcron.toml
#运行服务
./xcron_server
```



