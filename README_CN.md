[TOC]

### 分布式任务调度系统

xcron是基于raft开发的分布式任务调度系统，目前依然在内部性能优化阶段。[English]

#### 1.问题现象

- 支付系统每天凌晨1点进行前一天清算，每月1号进行上个月清算
- 电商整点抢购，商品价格8点整开始优惠
- 12306购票系统，超过30分钟没有成功支付订单的，进行回收处理

#### 2.功能需求

- 执行指定时间点进行任务触发
- 支持周期性任务触发

#### 3.系统特点

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
    "uuid": "80eb2fb8-2724-4707-9484-8e7d07b9171d",
    "code": 0,
    "data": {
        "task_id": "289057701458138645"
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
    "uuid":"411b94ed-0455-4f8b-85e0-f9a6c164ef39",
    "code":0
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
    "uuid": "347417d8-2067-4292-851b-6fc06f6869cd",
    "code": 0,
    "data": {
        "task_id": "289058143990765077"
    }
}
```

#### 6.使用说明

```shell
#1.项目根目录
bash build.sh
#2.修改配置文件 xcron.toml
# - 2.1 单机版可直接运行
# - 2.2 集群版需要修改配置文件如下:
# - peer_hosts = ["192.24.1.1:8899","192.24.1.2:8899","192.24.1.3:8899"]
#3.运行服务
./xcron_server
```

#### 7.压测报告

##### 7.1 ab压测准备

硬件配置:

```
 虚拟机: core: 4cpu; memory: 8g
```

ab命令:

```bash
ab -c 200 -n 10000 -T 'application/json' -p json.txt http://host:8000/xcron/createTask

#json.txt
{
	"type":1,
	"format":"1581823751",
	"callback":{
		"url":"http://test.demo.com:8810",
		"body":{
			"params":[1,2],
			"data":"10.10.10",
			"count":1
		}
	}
}
```

##### 7.2 压测结果分析

```bash
#1. ab结果
	Requests per second:    1982.60 [#/sec] (mean)
	Time per request:       100.878 [ms] (mean)
#2. cpu消耗
	10%左右
#3. 内存消耗
	队列堆积峰值:RES 105M
	队列消费结束:RES 59M
#4. 任务消费耗时
	任务总数:10000
	消费持续时间:180s
#5. 磁盘文件大小
	raft.log: 40M
```

##### 7.3 并行执行10000任务分析

```bash
#1. 10000任务并行执行
	持续时间: 12s
	raft.log文件大小: 41M
	物理内存RES: 131M
#2. 数据分析
	约100task/1M
```

