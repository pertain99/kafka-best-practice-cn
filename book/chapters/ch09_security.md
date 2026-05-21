# 第 9 章：安全与认证

## 本章你将学到

- Kafka 安全的四个层次及其防护边界
- TLS/SSL 配置：自签证书与生产证书的完整流程
- SASL 三种认证机制的对比与选型
- ACL（访问控制列表）精细化权限管理
- Python 客户端安全连接配置
- 生产环境安全 Checklist（10 条必查项）
- 动手：为本地 Docker 环境配置 SASL/PLAIN 认证

---

## 9.1 Kafka 安全四层

Kafka 的安全体系分为四个独立但互补的层次：

```
┌─────────────────────────────────────────────────────────────┐
│                      Kafka 安全四层                          │
│                                                             │
│  第4层：静态数据加密（Data Encryption at Rest）               │
│  ─────────────────────────────────────────────            │
│  存储在磁盘上的数据加密（通常由 OS/云服务商提供）               │
│                                                             │
│  第3层：授权（Authorization）                                │
│  ──────────────────────────                               │
│  谁能对哪些资源做什么操作？                                   │
│  工具：Kafka ACL、RBAC（企业版）                             │
│                                                             │
│  第2层：认证（Authentication）                               │
│  ─────────────────────────                               │
│  你是谁？证明你的身份。                                      │
│  工具：SASL/PLAIN、SASL/SCRAM、SASL/OAUTHBEARER             │
│                                                             │
│  第1层：传输加密（Encryption in Transit）                     │
│  ──────────────────────────────────────                   │
│  网络传输中的数据加密，防止窃听。                              │
│  工具：TLS/SSL                                              │
└─────────────────────────────────────────────────────────────┘
```

**最小安全配置建议**：

```
开发环境：不需要安全配置（节省配置复杂度）

测试/预生产：至少配置 SASL 认证（防止误连接）

生产环境：TLS + SASL + ACL 全部开启
```

---

## 9.2 TLS/SSL 配置

### 9.2.1 为什么需要 TLS

```
没有 TLS 的 Kafka 通信（明文）：
  Producer ──────────明文消息──────────→ Broker
  
  攻击者在网络上（如 man-in-the-middle）可以：
  1. 读取所有消息内容（窃听）
  2. 篡改消息（中间人攻击）
  3. 伪装成合法客户端（身份冒充）

有了 TLS 的 Kafka 通信（加密）：
  Producer ────────加密消息（TLS）─────→ Broker
  
  攻击者只能看到密文，无法解读和篡改。
```

### 9.2.2 证书体系

```
TLS 证书体系（信任链）：

CA（证书颁发机构）
 └── 签发 Kafka Broker 证书（server.keystore.jks）
 └── 签发 Kafka Client 证书（可选，双向 TLS）
 
Truststore：包含 CA 证书，用于验证对方证书是否由可信 CA 签发
Keystore：包含自己的证书和私钥，用于证明自己的身份

单向 TLS：客户端验证服务端证书（最常用）
双向 TLS（mTLS）：双方互相验证证书（更安全，适合服务间通信）
```

### 9.2.3 生成自签证书（开发/测试环境）

```bash
#!/bin/bash
# generate_ssl_certs.sh - 生成 Kafka SSL 自签证书
# 用于开发/测试环境。生产环境请使用 Let's Encrypt 或内部 CA

set -e

# ——— 配置变量 ———
CA_VALIDITY_DAYS=3650    # CA 证书有效期 10 年
CERT_VALIDITY_DAYS=365   # 服务器证书有效期 1 年
PASSWORD="kafka-ssl-password"  # 生产环境请使用强密码
KAFKA_HOSTNAME="localhost"     # Kafka Broker 的主机名（生产环境改为实际域名）
OUTPUT_DIR="./ssl"

mkdir -p $OUTPUT_DIR
cd $OUTPUT_DIR

echo "=== 步骤 1: 创建 CA（证书颁发机构）==="
# 生成 CA 私钥
openssl req -new -x509 \
  -keyout ca-key \
  -out ca-cert \
  -days $CA_VALIDITY_DAYS \
  -subj "/CN=kafka-ca/OU=Kafka/O=MyCompany/L=Edmonton/ST=AB/C=CA" \
  -passout pass:$PASSWORD

echo "=== 步骤 2: 创建 Broker Keystore（存放 Broker 证书和私钥）==="
# 生成 Broker 密钥对和自签证书
keytool -genkey -noprompt \
  -alias kafka-broker \
  -dname "CN=$KAFKA_HOSTNAME, OU=Kafka, O=MyCompany, L=Edmonton, ST=AB, C=CA" \
  -keystore kafka.server.keystore.jks \
  -keyalg RSA \
  -storepass $PASSWORD \
  -keypass $PASSWORD \
  -validity $CERT_VALIDITY_DAYS

echo "=== 步骤 3: 生成证书签名请求（CSR）==="
keytool -keystore kafka.server.keystore.jks \
  -alias kafka-broker \
  -certreq \
  -file cert-unsigned \
  -storepass $PASSWORD

echo "=== 步骤 4: 用 CA 签署 CSR，生成 Broker 证书 ==="
openssl x509 -req \
  -CA ca-cert \
  -CAkey ca-key \
  -in cert-unsigned \
  -out cert-signed \
  -days $CERT_VALIDITY_DAYS \
  -CAcreateserial \
  -passin pass:$PASSWORD

echo "=== 步骤 5: 导入 CA 证书和已签署的 Broker 证书到 Keystore ==="
# 先导入 CA 证书（建立信任链）
keytool -keystore kafka.server.keystore.jks \
  -alias CARoot \
  -import -file ca-cert \
  -storepass $PASSWORD \
  -noprompt

# 再导入已签署的 Broker 证书
keytool -keystore kafka.server.keystore.jks \
  -alias kafka-broker \
  -import -file cert-signed \
  -storepass $PASSWORD \
  -noprompt

echo "=== 步骤 6: 创建 Truststore（存放 CA 证书，供客户端验证 Broker）==="
keytool -keystore kafka.server.truststore.jks \
  -alias CARoot \
  -import -file ca-cert \
  -storepass $PASSWORD \
  -noprompt

# 客户端也需要 Truststore
keytool -keystore kafka.client.truststore.jks \
  -alias CARoot \
  -import -file ca-cert \
  -storepass $PASSWORD \
  -noprompt

echo "=== 完成！生成的文件 ==="
ls -la $OUTPUT_DIR
echo ""
echo "Broker 使用:  kafka.server.keystore.jks + kafka.server.truststore.jks"
echo "Client 使用:  kafka.client.truststore.jks"
echo "密码:         $PASSWORD"
```

### 9.2.4 Kafka Broker TLS 配置

```properties
# server.properties - TLS 相关配置

# ——— 监听器配置：同时支持 PLAINTEXT 和 SSL ———
# 生产环境：只保留 SSL，删除 PLAINTEXT
listeners=PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093
advertised.listeners=PLAINTEXT://your-broker-host:9092,SSL://your-broker-host:9093

# ——— SSL 证书配置 ———
ssl.keystore.location=/etc/kafka/ssl/kafka.server.keystore.jks
ssl.keystore.password=kafka-ssl-password
ssl.key.password=kafka-ssl-password
ssl.truststore.location=/etc/kafka/ssl/kafka.server.truststore.jks
ssl.truststore.password=kafka-ssl-password

# ——— TLS 协议版本（禁用不安全的旧版本）———
ssl.protocol=TLSv1.2
ssl.enabled.protocols=TLSv1.2,TLSv1.3

# ——— 加密套件（只允许强加密）———
# 使用 Java 默认即可，或显式指定安全套件
ssl.cipher.suites=TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256

# ——— 客户端认证模式 ———
# none        = 单向 TLS（只验证服务端证书）
# requested   = 可选双向 TLS
# required    = 强制双向 TLS（mTLS）
ssl.client.auth=none

# ——— Broker 间通信也使用 SSL ———
security.inter.broker.protocol=SSL
```

### 9.2.5 Python 客户端 SSL 配置

```python
# python_ssl_client.py - Python Kafka 客户端 SSL 配置
from kafka import KafkaProducer, KafkaConsumer
import ssl

# ——— 方法一：使用 JKS 文件（通过 ssl_context 配置）———
# 先将 JKS 转换为 PEM 格式（Python 的 ssl 模块使用 PEM）
# 转换命令：
# keytool -importkeystore -srckeystore kafka.client.truststore.jks \
#   -destkeystore kafka.client.truststore.p12 -deststoretype PKCS12 \
#   -srcstorepass kafka-ssl-password -deststorepass kafka-ssl-password
# openssl pkcs12 -in kafka.client.truststore.p12 -out ca-cert.pem \
#   -nokeys -passin pass:kafka-ssl-password

# ——— 方法二：直接使用 PEM 文件（更简洁）———

def create_ssl_context(
    ca_cert_file: str,              # CA 证书文件（.pem 格式）
    client_cert_file: str = None,   # 客户端证书（双向 TLS 需要）
    client_key_file: str = None,    # 客户端私钥（双向 TLS 需要）
    client_key_password: str = None,
) -> ssl.SSLContext:
    """
    创建 Kafka Python 客户端的 SSL 上下文
    
    Args:
        ca_cert_file: CA 证书路径，用于验证 Broker 证书
        client_cert_file: 客户端证书路径（双向 TLS 需要）
        client_key_file: 客户端私钥路径（双向 TLS 需要）
    
    Returns:
        配置好的 SSL Context
    """
    # 创建 SSL 上下文
    context = ssl.create_default_context()
    
    # 加载 CA 证书（用于验证 Broker 证书合法性）
    context.load_verify_locations(cafile=ca_cert_file)
    
    # 如果需要双向 TLS（mTLS），加载客户端证书
    if client_cert_file and client_key_file:
        context.load_cert_chain(
            certfile=client_cert_file,
            keyfile=client_key_file,
            password=client_key_password,
        )
    
    # 只允许 TLS 1.2 和 1.3（禁用旧版本）
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    
    return context


# ——— SSL Producer 配置 ———
ssl_context = create_ssl_context(ca_cert_file='./ssl/ca-cert.pem')

ssl_producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],  # SSL 端口（9093）
    security_protocol='SSL',
    ssl_context=ssl_context,
    value_serializer=lambda v: str(v).encode('utf-8'),
)

# ——— SSL Consumer 配置 ———
ssl_consumer = KafkaConsumer(
    'raw-trades',
    bootstrap_servers=['localhost:9093'],
    security_protocol='SSL',
    ssl_context=ssl_context,
    group_id='ssl-demo-group',
    auto_offset_reset='latest',
)

print("SSL 连接配置完成！")
```

---

## 9.3 SASL 认证机制

SASL（Simple Authentication and Security Layer，简单认证安全层）是 Kafka 支持的认证框架，提供三种主要机制。

### 9.3.1 三种 SASL 机制对比

```
┌─────────────────┬──────────────┬──────────────┬────────────────────┐
│ 机制            │ 安全级别      │ 适用场景     │ 复杂度              │
├─────────────────┼──────────────┼──────────────┼────────────────────┤
│ SASL/PLAIN      │ 低           │ 开发、内部   │ 极低               │
│                 │              │ 无 TLS 时不  │                    │
│                 │              │ 安全（明文）  │                    │
├─────────────────┼──────────────┼──────────────┼────────────────────┤
│ SASL/SCRAM-     │ 高           │ 生产推荐     │ 低                 │
│ SHA-256/512     │              │ 无需外部系统  │                    │
│                 │              │              │                    │
├─────────────────┼──────────────┼──────────────┼────────────────────┤
│ SASL/           │ 高           │ 云原生、K8s  │ 高（需 OAuth 服务器）│
│ OAUTHBEARER     │              │ 与 IAM 集成  │                    │
└─────────────────┴──────────────┴──────────────┴────────────────────┘
```

### 9.3.2 SASL/PLAIN：简单用户名密码认证

**适用场景**：开发环境、内部私有网络（必须配合 TLS 使用！）

**Broker 配置**：

```properties
# server.properties - SASL/PLAIN 配置

# 开启 SASL_PLAINTEXT（SASL + 明文传输，仅用于开发）
# 生产环境请使用 SASL_SSL（SASL + TLS 加密）
listeners=SASL_PLAINTEXT://0.0.0.0:9092
advertised.listeners=SASL_PLAINTEXT://localhost:9092

# 指定 SASL 机制
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN

# Broker 间通信使用 SASL
security.inter.broker.protocol=SASL_PLAINTEXT
```

**JAAS 配置文件**（Java 认证授权服务配置）：

```
# /etc/kafka/kafka_server_jaas.conf
# 定义 Kafka Broker 使用的用户名和密码

KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-password"
  user_admin="admin-password"
  user_producer_service="producer-password"
  user_consumer_service="consumer-password"
  user_risk_service="risk-password";
};

# 说明：
# username/password: Broker 间通信使用的凭证
# user_<username>="<password>": 定义客户端可使用的账号
```

**启动时加载 JAAS 配置**：

```bash
# 在 Kafka 启动脚本中加入 JVM 参数
export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf"
./bin/kafka-server-start.sh ./config/server.properties
```

**Python 客户端 SASL/PLAIN 配置**：

```python
# sasl_plain_client.py
from kafka import KafkaProducer, KafkaConsumer

# SASL/PLAIN Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    security_protocol='SASL_PLAINTEXT',  # 开发用；生产用 SASL_SSL
    sasl_mechanism='PLAIN',
    sasl_plain_username='producer_service',
    sasl_plain_password='producer-password',
    value_serializer=lambda v: str(v).encode('utf-8'),
)

# SASL/PLAIN Consumer
consumer = KafkaConsumer(
    'raw-trades',
    bootstrap_servers=['localhost:9092'],
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='consumer_service',
    sasl_plain_password='consumer-password',
    group_id='secure-consumer-group',
)

print("SASL/PLAIN 连接成功！")
```

### 9.3.3 SASL/SCRAM：安全挑战响应认证（生产推荐）

SCRAM（Salted Challenge Response Authentication Mechanism）相比 PLAIN 的优势：
- 密码经过哈希处理，即使传输中被截获也无法使用
- 支持在线添加/删除用户，无需重启 Broker
- 密码存储在 ZooKeeper/KRaft 中，Broker 无需重启即可生效

```bash
# 1. 配置 Broker 使用 SCRAM
# server.properties
sasl.enabled.mechanisms=SCRAM-SHA-256
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
security.inter.broker.protocol=SASL_SSL
listeners=SASL_SSL://0.0.0.0:9093
```

```bash
# 2. 创建用户（在 ZooKeeper/KRaft 中存储加盐哈希）

# 创建 admin 用户
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=admin-strong-pass]' \
  --entity-type users \
  --entity-name admin

# 创建 producer-service 用户
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=prod-svc-pass-2024!]' \
  --entity-type users \
  --entity-name producer-service

# 创建 risk-service 用户（只读权限）
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=risk-svc-pass-2024!]' \
  --entity-type users \
  --entity-name risk-service

# 查看已配置的用户
kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe \
  --entity-type users

# 删除用户
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter \
  --delete-config 'SCRAM-SHA-256' \
  --entity-type users \
  --entity-name old-user
```

```
# /etc/kafka/kafka_server_jaas.conf - SCRAM 版本
KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="admin"
  password="admin-strong-pass";
};
```

**Python 客户端 SCRAM 配置**：

```python
# sasl_scram_client.py
from kafka import KafkaProducer, KafkaConsumer
import ssl

# 创建 SSL Context（SCRAM 必须配合 TLS 使用）
ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(cafile='./ssl/ca-cert.pem')

# SCRAM Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    security_protocol='SASL_SSL',       # SASL + TLS（生产标准）
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username='producer-service',
    sasl_plain_password='prod-svc-pass-2024!',
    ssl_context=ssl_context,
    value_serializer=lambda v: str(v).encode('utf-8'),
)

# SCRAM Consumer
consumer = KafkaConsumer(
    'raw-trades',
    bootstrap_servers=['localhost:9093'],
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-256',
    sasl_plain_username='risk-service',
    sasl_plain_password='risk-svc-pass-2024!',
    ssl_context=ssl_context,
    group_id='risk-service-group',
)

print("SASL/SCRAM-SHA-256 + TLS 连接成功！")
```

### 9.3.4 SASL/OAUTHBEARER：云原生 OAuth2 认证

**适用场景**：Kubernetes 环境、与云 IAM（Identity Access Management）集成。

```
OAuth2 认证流程：

  1. 客户端 → OAuth 服务器：请求 Access Token
     （携带 client_id + client_secret）
  
  2. OAuth 服务器 → 客户端：返回 JWT Token（有效期 1 小时）
  
  3. 客户端 → Kafka Broker：携带 JWT Token 连接
     （SASL OAUTHBEARER 握手）
  
  4. Kafka Broker → OAuth 服务器：验证 Token 合法性（可选）
  
  5. 认证成功 → 建立连接
```

```python
# sasl_oauth_client.py - OAuth Bearer Token 认证
import time
import requests
from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider

class OAuthTokenProvider(AbstractTokenProvider):
    """
    自定义 OAuth Token Provider
    从 OAuth 服务器动态获取和刷新 Token
    """
    
    def __init__(self, token_endpoint: str, client_id: str, client_secret: str):
        self.token_endpoint = token_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._token_expiry = 0
    
    def token(self) -> str:
        """
        返回有效的 Bearer Token
        如果 Token 已过期（或即将过期），自动刷新
        """
        # 提前 60 秒刷新（Token 快过期时）
        if time.time() >= self._token_expiry - 60:
            self._refresh_token()
        return self._token
    
    def _refresh_token(self):
        """从 OAuth 服务器获取新 Token"""
        response = requests.post(
            self.token_endpoint,
            data={
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'kafka:produce kafka:consume',
            },
            timeout=10,
        )
        response.raise_for_status()
        
        token_data = response.json()
        self._token = token_data['access_token']
        # expires_in 是 Token 有效秒数
        self._token_expiry = time.time() + token_data['expires_in']

# OAuth Producer
token_provider = OAuthTokenProvider(
    token_endpoint='https://auth.yourcompany.com/oauth/token',
    client_id='kafka-producer-service',
    client_secret='your-client-secret',
)

producer = KafkaProducer(
    bootstrap_servers=['kafka.yourcompany.com:9093'],
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=token_provider,
    # ssl_context=... （生产环境加上 TLS 配置）
)
```

---

## 9.4 ACL：访问控制列表

ACL（Access Control List，访问控制列表）是 Kafka 的授权机制，精确控制"谁"能对"哪些资源"执行"什么操作"。

### 9.4.1 ACL 三要素

```
Principal（主体）= 谁
  User:risk-service       ← 用户账号
  Group:data-team         ← 用户组（企业版支持）

Resource（资源）= 什么资源
  Topic:raw-trades        ← 特定 Topic
  Topic:*                 ← 所有 Topic（通配符）
  ConsumerGroup:risk-*    ← 匹配模式的 Consumer Group
  Cluster                 ← 整个集群

Operation（操作）= 什么操作
  Read       ← 消费消息
  Write      ← 生产消息
  Create     ← 创建 Topic
  Delete     ← 删除 Topic
  Describe   ← 查看 Topic 元数据
  Alter      ← 修改 Topic 配置
  All        ← 所有操作
```

### 9.4.2 Broker 开启 ACL

```properties
# server.properties - 开启 ACL 授权
# 指定 Authorizer 类（Kafka 内置的 ACL 实现）
authorizer.class.name=kafka.security.authorizer.AclAuthorizer

# 超级用户（可以绕过 ACL，用于管理员）
# 格式：User:username;User:another-user
super.users=User:admin

# 当没有 ACL 规则时的默认行为
# true  = 允许（宽松模式，用于迁移期间）
# false = 拒绝（严格模式，生产推荐）
allow.everyone.if.no.acl.found=false
```

### 9.4.3 常用 ACL 管理命令

```bash
# ——— 赋予 Producer 服务写入权限 ———

# producer-service 可以向 raw-trades topic 写入消息
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:producer-service \
  --operation Write \
  --topic raw-trades

# producer-service 可以向 raw-trades topic 描述（读取元数据）
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:producer-service \
  --operation Describe \
  --topic raw-trades


# ——— 赋予 risk-service 只读权限 ———

# risk-service 可以读取 raw-trades topic
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:risk-service \
  --operation Read \
  --topic raw-trades

# risk-service 可以使用 risk-service-group Consumer Group
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin.properties \
  --add \
  --allow-principal User:risk-service \
  --operation Read \
  --group risk-service-group

# ——— 赋予 admin 用户全部权限 ———
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:admin \
  --operation All \
  --topic '*'  \
  --cluster

# ——— 查看已设置的 ACL ———
# 查看 raw-trades topic 的所有 ACL
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --list \
  --topic raw-trades

# 查看某个用户的所有 ACL
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --list \
  --principal User:risk-service

# ——— 删除 ACL ———
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --remove \
  --allow-principal User:old-service \
  --operation Read \
  --topic raw-trades
```

### 9.4.4 ACL 设计原则：最小权限

```
最小权限原则（Principle of Least Privilege）：
  每个服务只授予完成其工作所需的最小权限
  
  交易处理系统的 ACL 设计示例：
  
  producer-service:
    ✅ Write  → raw-trades
    ✅ Describe → raw-trades
    ❌ Read   → raw-trades（不需要读自己发的消息）
    ❌ Write  → processed-trades（不应绕过处理步骤）
  
  risk-service:
    ✅ Read   → raw-trades
    ✅ Read   → consumer group: risk-service-*
    ✅ Write  → risk-alerts（写告警）
    ❌ Write  → raw-trades（风控服务不应该写原始交易）
    ❌ Delete → *（任何服务都不应有删除权限）
  
  monitoring-service:
    ✅ Describe → *（只需要读取 Topic 元数据）
    ❌ Read/Write → *（监控不需要读写业务数据）
  
  data-admin:
    ✅ All → *（管理员，但应限制使用场景）
```

---

## 9.5 动手练习：为 Docker 环境配置 SASL/PLAIN

### 目标

在本地 Docker 环境中启动一个配置了 SASL/PLAIN 认证的 Kafka，并验证认证生效（未认证的连接被拒绝）。

### 步骤一：Docker Compose 配置

```yaml
# docker-compose-sasl.yml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      # ZooKeeper 也需要配置 SASL（企业级安全）
      # 简单起见，ZooKeeper 用无认证（只用于本地开发）
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      
      # ——— SASL/PLAIN 配置 ———
      KAFKA_LISTENERS: SASL_PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: SASL_PLAINTEXT://localhost:9092
      KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SASL_PLAINTEXT
      KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
      KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
      
      # ——— ACL 配置 ———
      KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
      KAFKA_SUPER_USERS: "User:admin"
      KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
      
      # ——— JAAS 配置（用户名密码）———
      # Confluent 镜像通过环境变量支持 JAAS 配置
      KAFKA_OPTS: >-
        -Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf
    
    volumes:
      - ./sasl/kafka_server_jaas.conf:/etc/kafka/secrets/kafka_server_jaas.conf:ro
```

### 步骤二：创建 JAAS 配置文件

```bash
# 创建配置目录
mkdir -p ./sasl

# 创建 JAAS 文件
cat > ./sasl/kafka_server_jaas.conf << 'EOF'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_producer-service="producer-secret"
  user_risk-service="risk-secret";
};
EOF

echo "JAAS 配置文件创建完成"
cat ./sasl/kafka_server_jaas.conf
```

### 步骤三：启动并配置 ACL

```bash
# 启动 Kafka
docker-compose -f docker-compose-sasl.yml up -d

# 等待 Kafka 就绪（约 30 秒）
sleep 30

# 创建 admin 客户端配置文件（admin 是超级用户，可以绕过 ACL）
cat > ./sasl/admin.properties << 'EOF'
bootstrap.servers=localhost:9092
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="admin" \
  password="admin-secret";
EOF

# 创建测试 Topic（以 admin 身份）
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --command-config ./sasl/admin.properties \
  --create \
  --topic raw-trades \
  --partitions 3 \
  --replication-factor 1

# 设置 ACL：producer-service 可以写入
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config ./sasl/admin.properties \
  --add \
  --allow-principal "User:producer-service" \
  --operation Write \
  --operation Describe \
  --topic raw-trades

# 设置 ACL：risk-service 可以读取
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config ./sasl/admin.properties \
  --add \
  --allow-principal "User:risk-service" \
  --operation Read \
  --operation Describe \
  --topic raw-trades

kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config ./sasl/admin.properties \
  --add \
  --allow-principal "User:risk-service" \
  --operation Read \
  --group "risk-service-group"

echo "ACL 配置完成！"

# 查看 ACL 设置
kafka-acls.sh \
  --bootstrap-server localhost:9092 \
  --command-config ./sasl/admin.properties \
  --list \
  --topic raw-trades
```

### 步骤四：验证认证与授权

```python
# verify_sasl.py - 验证 SASL 认证效果
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import TopicAuthorizationFailedError, NoBrokersAvailable
import json
import time

def test_connection(username: str, password: str, operation: str):
    """测试特定用户的连接和权限"""
    print(f"\n=== 测试用户: {username}, 操作: {operation} ===")
    
    common_config = {
        'bootstrap_servers': ['localhost:9092'],
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username': username,
        'sasl_plain_password': password,
    }
    
    if operation == 'produce':
        try:
            producer = KafkaProducer(
                **common_config,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            future = producer.send('raw-trades', value={'test': 'message'})
            future.get(timeout=10)
            print(f"✅ 生产成功！用户 {username} 有写入权限")
            producer.close()
        except TopicAuthorizationFailedError:
            print(f"❌ 权限拒绝！用户 {username} 没有写入 raw-trades 的权限")
        except Exception as e:
            print(f"❌ 错误: {type(e).__name__}: {e}")
    
    elif operation == 'consume':
        try:
            consumer = KafkaConsumer(
                'raw-trades',
                **common_config,
                group_id='risk-service-group' if username == 'risk-service' else 'test-group',
                consumer_timeout_ms=3000,  # 3 秒后超时
            )
            count = 0
            for msg in consumer:
                count += 1
                if count >= 1:
                    break
            consumer.close()
            print(f"✅ 消费成功！用户 {username} 有读取权限（收到 {count} 条消息）")
        except TopicAuthorizationFailedError:
            print(f"❌ 权限拒绝！用户 {username} 没有读取 raw-trades 的权限")
        except Exception as e:
            print(f"❌ 错误: {type(e).__name__}: {e}")

# ——— 验证测试 ———

print("=== Kafka SASL/PLAIN 认证验证测试 ===\n")

# 测试 1：producer-service 可以写入（应该成功）
test_connection('producer-service', 'producer-secret', 'produce')

# 测试 2：producer-service 不能读取（应该失败）
test_connection('producer-service', 'producer-secret', 'consume')

# 测试 3：risk-service 可以读取（应该成功）
test_connection('risk-service', 'risk-secret', 'consume')

# 测试 4：risk-service 不能写入（应该失败）
test_connection('risk-service', 'risk-secret', 'produce')

# 测试 5：未知用户被拒绝（应该失败）
test_connection('unknown-user', 'wrong-password', 'produce')

print("\n=== 验证完成 ===")
```

**预期输出**：

```
=== Kafka SASL/PLAIN 认证验证测试 ===

=== 测试用户: producer-service, 操作: produce ===
✅ 生产成功！用户 producer-service 有写入权限

=== 测试用户: producer-service, 操作: consume ===
❌ 权限拒绝！用户 producer-service 没有读取 raw-trades 的权限

=== 测试用户: risk-service, 操作: consume ===
✅ 消费成功！用户 risk-service 有读取权限（收到 1 条消息）

=== 测试用户: risk-service, 操作: produce ===
❌ 权限拒绝！用户 risk-service 没有写入 raw-trades 的权限

=== 测试用户: unknown-user, 操作: produce ===
❌ 错误: NoBrokersAvailable: 认证失败

=== 验证完成 ===
```

---

## 9.6 生产安全 Checklist

在将 Kafka 集群部署到生产环境之前，逐一核查以下 10 条安全配置：

```
Kafka 生产安全 Checklist

必须项（P0）：
☐ 1. 传输加密：所有外部连接使用 TLS 1.2+
       listeners 中不包含 PLAINTEXT://（只有 SASL_SSL://）
       
☐ 2. SASL 认证：所有客户端使用 SASL/SCRAM 或 OAUTHBEARER
       禁止使用 SASL/PLAIN（明文密码）
       
☐ 3. ACL 开启：authorizer.class.name 已配置
       allow.everyone.if.no.acl.found=false（严格模式）
       
☐ 4. 最小权限：每个服务只有完成工作所需的最小 ACL
       定期审计 ACL，删除不再使用的权限
       
☐ 5. 超级用户控制：super.users 只包含运维管理账号
       禁止应用服务使用超级用户账号

重要项（P1）：
☐ 6. 证书管理：使用受信任的 CA 签发证书（非自签）
       设置证书到期监控告警（提前 30 天提醒）
       
☐ 7. 密码强度：所有 SASL 密码符合密码策略
       定期轮换密码（或使用 OAUTHBEARER 避免长期密码）
       
☐ 8. ZooKeeper 安全：ZooKeeper 也配置 SASL 认证
       禁止 ZooKeeper 暴露到公网
       
☐ 9. 网络隔离：Kafka Broker 只在内网可访问
       使用 VPC/防火墙限制端口访问（9092/9093 只对内部服务开放）

建议项（P2）：
☐ 10. 审计日志：开启 Kafka 授权日志
        log4j.logger.kafka.authorizer.logger=INFO, authorizerAppender
        记录所有 ACL 拒绝事件，便于安全审计
```

---

## 本章小结

| 安全层 | 技术 | 适用场景 |
|--------|------|---------|
| 传输加密 | TLS/SSL | 所有生产环境 |
| 认证 - 开发 | SASL/PLAIN | 内网开发，必须配合 TLS |
| 认证 - 生产 | SASL/SCRAM-SHA-256 | 大多数生产场景 |
| 认证 - 云原生 | SASL/OAUTHBEARER | K8s、云 IAM 集成 |
| 授权 | ACL | 精细化权限控制 |

**最重要的三条原则**：
1. **明文密码绝不上生产**（PLAIN 只在内网+TLS 下使用）
2. **最小权限**（每个服务只有刚够用的权限）
3. **纵深防御**（TLS + SASL + ACL 三层叠加）

下一章，我们将深入**生产级运维与调优**——如何让 Kafka 集群在生产环境中稳定高效地运行？
