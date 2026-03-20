# Monika - 一站式风控数据洞察平台

一个功能完整的风控数据查询平台，支持多维度数据查询和分析。

## ✨ 最新更新

**🔗 KGraph 图查询** - 新增知识图谱查询可视化功能 (2026-01-23)
- 支持标准 Cypher 查询语法
- D3.js 力导向图可视化
- 多跳关系图展示
- 节点/边属性交互查看
- [功能文档](./docs/KGRAPH_QUERY_FEATURE.md) | [快速开始](./docs/KGRAPH_QUICKSTART.md)

**🔍 单用户风险排查** - 单用户多维度信息查询功能
> **[v1.2.0 发布说明](https://docs.corp.kuaishou.com/k/home/VWhjCR7rP4Go/fcABB1ng3zc9YxJWDtQJcIC4I)** (2026-01-21) - 设备风险查询、自动填充升级、稳定性修复。

**🔍 单用户风险排查** - 新增单用户多维度信息查询功能
- 通过用户ID查询 ClickHouse 数据库
- 自动分区查询（默认昨天）
- 多维度信息展示：基础、设备、网络、社交
- [快速开始](./docs/guides/QUICKSTART.md)

## 技术栈

- **React 18** - UI 框架
- **TypeScript** - 类型安全
- **Vite** - 构建工具
- **React Router** - 路由管理
- **D3.js** - 图可视化（新增）
- **Express** - 后端API服务器
- **内部SQL技能** - Hive/ClickHouse/MySQL查询

## 功能特性

### 核心功能
- 🗺️ **站点地图式侧边栏** - 树形菜单结构，支持展开/收起
- 📊 **数据查询页面** - 可复用的数据表格组件
- 🔍 **搜索功能** - 实时搜索过滤数据
- 📄 **分页功能** - 完整的分页导航
- 🎨 **响应式设计** - 支持深色/浅色主题
- ⚡ **高性能** - 使用 Vite 构建，开发体验流畅

### 图查询功能（新增）
- 🔗 **Cypher 查询** - 支持标准 Cypher 查询语法
- 🌐 **图可视化** - D3.js 力导向图布局
- 🎨 **节点类型** - 不同颜色标识不同类型
- 🖱️ **交互操作** - 拖拽、缩放、属性查看
- 📈 **多跳关系** - 支持多层级关系展示

## 项目结构

```
monika/
├── src/
│   ├── components/          # 公共组件
│   │   ├── Sidebar/        # 侧边栏导航组件
│   │   └── DataQuery/      # 数据查询表格组件
│   ├── pages/              # 页面组件
│   │   ├── DashboardPage.tsx
│   │   ├── UsersPage.tsx
│   │   ├── ProductListPage.tsx
│   │   ├── OrdersPage.tsx
│   │   └── GenericPage.tsx
│   ├── types/              # TypeScript 类型定义
│   ├── utils/              # 工具函数
│   │   ├── menuData.ts    # 菜单配置
│   │   └── mockData.ts    # 模拟数据工具
│   ├── styles/             # 全局样式
│   ├── App.tsx            # 应用主组件
│   └── main.tsx           # 应用入口
├── package.json
├── tsconfig.json
├── vite.config.ts
└── index.html
```

## 快速开始

### 方式一：完整启动（推荐）

同时启动前端和后端服务器：

```bash
# 1. 安装依赖
npm install

# 2. 同时启动前后端
npm run dev:all
```

### 访问地址

#### 本地访问
- 前端：http://localhost:3000
- 后端API：http://localhost:3001

#### 通过 IP 访问（局域网）

**查看本机访问地址：**
```bash
./show-access-url.sh
```

输出示例：
```
🌐 访问地址：
  本地访问：http://localhost:3000
  局域网访问：http://172.24.133.133:3000
```

**或手动查看 IP：**
```bash
# macOS/Linux
ifconfig | grep "inet " | grep -v 127.0.0.1

# Windows
ipconfig
```

然后使用 `http://YOUR_IP:3000` 访问

> 💡 **提示**：
> - 系统已配置支持 IP 访问（监听 0.0.0.0）
> - 确保防火墙允许 3000 和 3001 端口
> - 确保设备在同一局域网内
> - 详细配置请查看 [START_GUIDE.md](./docs/guides/START_GUIDE.md)

### 方式二：仅前端开发

```bash
npm install
npm run dev
```

### 方式三：分别启动

```bash
# 终端1 - 启动后端
npm run server

# 终端2 - 启动前端
npm run dev
```

### 构建和预览

### 构建和预览

```bash
# 构建生产版本
npm run build

# 预览生产构建
npm run preview
```

## 可用命令

| 命令 | 说明 |
|------|------|
| `npm run dev:all` | 同时启动前后端（推荐） |
| `npm run dev` | 仅启动前端开发服务器 |
| `npm run server` | 仅启动后端API服务器 |
| `npm run build` | 构建生产版本 |
| `npm run preview` | 预览生产构建 |
| `npm run test:api` | 测试后端API |
| `./check-setup.sh` | 检查环境配置 |
| `./show-access-url.sh` | 显示访问地址（含局域网IP） |

## 页面说明

- **🔍 单用户风险排查** (`/single-user-risk`) - 单用户多维度信息查询 **[首页]**
- **🖥️ LB模型部署查询** (`/lb-model-deployment`) - LB模型部署信息查询

## 自定义开发

### 添加新菜单

在 `src/utils/menuData.ts` 中添加菜单项：

```typescript
{
  id: 'your-menu',
  title: '你的菜单',
  path: '/your-path',
  icon: '🎯',
}
```

### 创建数据查询页面

使用 `DataQuery` 组件快速创建数据页面：

```typescript
import DataQuery from '../components/DataQuery';

const YourPage = () => {
  const fetchData = async (params) => {
    // 实现数据获取逻辑
    return { data: [], total: 0 };
  };

  return (
    <DataQuery
      title="你的页面标题"
      columns={[
        { key: 'id', title: 'ID' },
        { key: 'name', title: '名称' },
      ]}
      fetchData={fetchData}
    />
  );
};
```

## 📚 详细文档

所有文档已整理到 **[docs/](./docs/)** 目录，按类型分类：

- **[📘 使用指南](./docs/guides/)** - 快速开始、配置指南、构建指南
- **[🔧 技术规范](./docs/specs/)** - 系统设计规范、API 文档
- **[📊 功能总结](./docs/summaries/)** - 功能实现总结、优化文档
- **[🗄️ 历史存档](./docs/archive/)** - 历史文档和迁移记录

**快速链接：**
- **[快速启动指南](./docs/guides/QUICKSTART.md)** - 5分钟快速上手
- **[IP访问配置指南](./docs/guides/START_GUIDE.md)** - 局域网访问配置和故障排查
- **[构建指南](./docs/guides/BUILD_GUIDE.md)** - 构建和部署说明
- **[分区选择器规范](./docs/specs/PARTITION_SELECTOR_SPEC.md)** - 分区系统技术规范

## 🌐 网络访问配置

系统已配置支持通过 IP 访问：

### 前端配置（vite.config.ts）
```typescript
server: {
  host: '0.0.0.0',  // 允许通过 IP 访问
  port: 3000,
  // ...
}
```

### 后端配置（server/index.js）
```javascript
// CORS 允许所有来源
res.header('Access-Control-Allow-Origin', '*');

// 监听所有网络接口
app.listen(PORT, '0.0.0.0', () => { ... });
```

详细配置和故障排查请参考 [START_GUIDE.md](./docs/guides/START_GUIDE.md)

## License

MIT
