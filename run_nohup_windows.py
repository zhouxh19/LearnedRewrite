#!/usr/bin/python
# encoding=utf-8
import datetime
import os
import threading

def execCmd(cmd):
    try:
        print("命令%s开始运行%s" % (cmd, datetime.datetime.now()))
        os.system(cmd)
        print("命令%s结束运行%s" % (cmd, datetime.datetime.now()))
    except:
        print('%s\t 运行失败' % (cmd))


if __name__ == '__main__':
    # 是否需要并行运行
    if_parallel = True

    # 需要执行的命令列表
    # model_list = ['yolo', 'centernet']
    cmds = ['start javaw -jar .\\output_dir\\artifacts\\sql_rewriter_bg_java_jar\\sql_rewriter_bg_java.jar '+str(i+1)+ " " + str(i+100) for i in range(0,1000,100)]
    # print(cmds)
    # exit()
    if if_parallel:
        # 并行
        threads = []
        for cmd in cmds:
            th = threading.Thread(target=execCmd, args=(cmd,))
            th.start()
            threads.append(th)

        # 等待线程运行完毕
        for th in threads:
            th.join()
    else:
        # 串行
        for cmd in cmds:
            try:
                print("命令%s开始运行%s" % (cmd, datetime.datetime.now()))
                os.system(cmd)
                print("命令%s结束运行%s" % (cmd, datetime.datetime.now()))
            except:
                print('%s\t 运行失败' % (cmd))
