# ==================== 第一阶段：构建依赖（减小最终镜像体积）====================
# 基础镜像：使用官方 Python 镜像（3.11 为长期支持版本，可替换为 3.9/3.10 等）
FROM python:3.11-slim AS builder

# 设置工作目录（容器内）
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - && apt-get install -y nodejs

# 复制依赖清单（利用 Docker 缓存：仅当 requirements.txt 变更时才重新安装依赖）
COPY CAO /app/CAO

# 升级 pip + 安装依赖到指定目录（便于后续复制，避免冗余）
RUN cd /app/CAO && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --target=/app/deps -r requirements.txt

RUN cd /app/CAO/hy && npm install && npm install -g tsx

# ==================== 第二阶段：最终运行镜像（轻量）====================
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - && apt-get install -y nodejs

# 配置 Python 路径（让 Python 能找到依赖包）
ENV PYTHONPATH=/app:/app/deps
ENV PATH=$PATH:/app/CAO/hy/node_modules/.bin

# 从构建阶段复制依赖（仅复制必要的依赖文件，减小镜像体积）
COPY --from=builder /app/deps ./deps
COPY --from=builder /app/CAO ./CAO

# 启动命令
CMD ["python","/app/CAO/all_data_jobs.py"]
# ENTRYPOINT ["python","/app/CAO/all_data_jobs.py"]