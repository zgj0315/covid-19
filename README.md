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
- [ ] clickhouse安装部署
- [ ] grafana安装部署
- [ ] rust程序设计开发
- [ ] dashboard设计开发
