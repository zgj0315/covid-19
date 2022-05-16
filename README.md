# 新冠疫情数据分析
## 数据来源
https://github.com/CSSEGISandData/COVID-19
## 技术组件
ClickHouse, Grafana
## 开发语言
RUST
## 业务流程
1. git pull最新的新冠数据
2. rust程序读取新冠数据，入库到clickhouse
3. grafana进行数据可视化

## TodoList
- [x] clickhouse安装部署
- [x] grafana安装部署
- [x] rust程序设计开发
- [x] dashboard设计开发
## 技术方案
### rust程序设计
- [x] 异步架构
- [x] 单线程运行
- [x] rust操作clickhouse的三方包选型

### clieckhouse安装部署
```
# 下载程序
curl -C - -O https://builds.clickhouse.com/master/macos/clickhouse

# 设置权限
chmod a+x clickhouse

# 启动程序
./clickhouse server

# 启动client
./clickhouse client

# 创建数据库
CREATE DATABASE IF NOT EXISTS covid_19

### grafana安装部署
```
# 下载程序
curl -C - -O https://dl.grafana.com/enterprise/release/grafana-enterprise-8.5.2.darwin-amd64.tar.gz

# 解压程序
tar zxvf grafana-enterprise-8.5.2.darwin-amd64.tar.gz

# 启动程序
./bin/grafana-server web

# 登陆Web
http://127.0.0.1:3000
admin/admin

```