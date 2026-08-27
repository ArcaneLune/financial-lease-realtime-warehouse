#!/bin/bash
# =====================================================
# Doris 集群启停脚本（FE + BE）
# 节点规划：hadoop100 = FE（Frontend），hadoop101/102 = BE（Backend）
# 用法：sh doris-cluster.sh {start|stop|status}
#
# 注意：
#   1. 脚本用 SSH 免密登录到各节点执行，需先配置 hadoop100 到 101/102 的免密
#   2. DORIS_HOME 按实际安装路径修改（默认 /opt/module/doris）
#   3. 上传到 Linux 后先执行：chmod +x doris-cluster.sh
# =====================================================

# ===== 可修改配置 =====
DORIS_HOME=/opt/module/doris          # Doris 安装目录（按实际改）
FE_HOST=hadoop100                     # FE 节点
BE_HOSTS="hadoop101 hadoop102"        # BE 节点（空格分隔）
# =====================

case "$1" in
  start)
    echo "===== 启动 Doris FE ($FE_HOST) ====="
    ssh "$FE_HOST" "$DORIS_HOME/fe/bin/start_fe.sh --daemon"
    # FE 启动后稍等，等 FE 就绪再起 BE（避免 BE 连不上 FE）
    sleep 10
    for host in $BE_HOSTS; do
      echo "===== 启动 Doris BE ($host) ====="
      ssh "$host" "$DORIS_HOME/be/bin/start_be.sh --daemon"
    done
    echo "Doris 启动完成！验证：mysql -h $FE_HOST -P 9030 -uroot -proot"
    ;;

  stop)
    for host in $BE_HOSTS; do
      echo "===== 停止 Doris BE ($host) ====="
      ssh "$host" "$DORIS_HOME/be/bin/stop_be.sh"
    done
    echo "===== 停止 Doris FE ($FE_HOST) ====="
    ssh "$FE_HOST" "$DORIS_HOME/fe/bin/stop_fe.sh"
    echo "Doris 已停止"
    ;;

  status)
    echo "===== Doris 进程状态 ====="
    for host in $FE_HOST $BE_HOSTS; do
      echo "--- $host ---"
      ssh "$host" "ps -ef | grep -E 'doris|palo' | grep -v grep | head -10"
    done
    echo "===== FE 状态（mysql 客户端检查）====="
    mysql -h "$FE_HOST" -P 9030 -uroot -proot -e "SHOW FRONTENDS; SHOW BACKENDS;" 2>/dev/null \
      || echo "无法连接 Doris FE（9030），请检查 FE 是否启动"
    ;;

  *)
    echo "用法: $0 {start|stop|status}"
    exit 1
    ;;
esac
