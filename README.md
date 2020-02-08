### 分布式任务调度系统

#### 1.问题现象

- 支付系统每天凌晨1点进行前一天清算，每月1号进行上个月清算
- 电商整点抢购，商品价格8点整开始优惠
- 12306购票系统，超过30分钟没有成功支付订单的，进行回收处理

#### 2.功能需求

- 执行指定时间点进行任务触发
- 支持周期性任务触发

#### 3.系统特点

- 高可用
- 强一致性
- 故障转移
- 大数据
- 快速扩容
- 监控及限流(TODO)

#### 4.系统设计

(1). 系统架构

<center>
    <img src="https://github.com/alwaysthanks/xcron/blob/master/doc/xcron_arch.png">
</center>

(2). 部署架构

<center>
    <img src="https://github.com/alwaysthanks/xcron/blob/master/doc/cluster_arch.png">
</center>


#### 5.系统分析

1.组组之间:

```
1.组长master决定组组之间哈希方案：数据模10240分片
2.组组扩容
```

2.组内成员:

```
1.组内leader判断该组是否工作
2.组内leader给出内部调度方案
3.leader提供分布式锁
4.组内个体给出迁移及恢复方案
6.个体监控及leader同步
7.分代计数分片
8.组内扩容
```

#### 6.对外接口(HTTP)

对外提供HTTP协议**content-type: application/json**相关接口:

##### 6.1 创建任务

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
##### 6.2 删除任务

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
##### 6.3 查询任务

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

#### 7.部署模式

(1).node模式(TODO)

```
优点:
1.并发能力强
2.可使用lvs进行集群部署

缺点:
1.单节点，节点故障无法转移
```

(2).master-master模式(TODO)

```
优点:
有一定的并发能力
组内故障转移

缺点:
组分化，组间无通信, 集群任务分配不均
```

(3).master-slave模式(TODO)

```
优点:
推荐3-5台为一组
组间通信，任务集群分配均匀
组内故障转移

缺点:
影响并发能力
```

