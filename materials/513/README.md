# 起点与运行

production起点deadletter_service.py是一发一收正常路径；event_loader.py对一次有效事件加值。持久投递/死信/重驱与效果去重/CAS/分类均是本题工作。

运行：`python3.11 demo.py`。demo仅展示材料已有正常能力；所有写入在临时目录，材料源文件保持只读。完整目标与合法输入见本题题面。
