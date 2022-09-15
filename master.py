from collections import Counter
import rpyc
import uuid
import math
import configparser
import signal
import pickle
import sys
import os
from rpyc.utils.server import ThreadedServer


def int_handler(signal, frame):
    """
    用来检查退出时，及时保存服务器上的元数据
    主要存放两个映射表 文件名->chunk_id
    :param signal:
    :param frame:
    :return:
    """
    with open('./master/metadata', 'wb') as fp:
        saved_data = (MasterService.exposed_Master.file_table, MasterService.exposed_Master.block_table)
        pickle.dump(saved_data, fp)
    sys.exit(0)


def init():
    conf = configparser.ConfigParser()
    with open('gfs.init') as fp:
        conf.read_file(fp)
    MasterService.exposed_Master.block_size = eval(conf.get('master', 'block_size'))
    MasterService.exposed_Master.replication = eval(conf.get('master', 'replication'))
    MasterService.exposed_Master.port = eval(conf.get('master', 'port'))
    sessions = conf.sections()
    for i in range(1, len(sessions)):  # pass master
        c = conf.get(sessions[i], 'ip')
        ip, port = c.split(":")
        MasterService.exposed_Master.chunks[i] = (ip, eval(port))
    if os.path.isfile('./master/metadata'):
        with open('./master/metadata', 'rb') as fp:
            MasterService.exposed_Master.file_table, MasterService.exposed_Master.block_table = pickle.load(fp)


class MasterService(rpyc.Service):
    class exposed_Master():
        file_table = {}  # 存放每个文件所在的位置，即block的映射关系 {filename:[block_id1,block_id2,....],....]}
        block_table = {}  # 存放block_id和chunk的映射关系，即每个block存放在哪些chunk上{block_id:[chunk_no1,chunk_no2...],...}
        chunks = {}  # 存放每个chunk对应的ip {chunk_no:(ip,port),.....}
        block_size = 64  # default 64B
        replication = 2  # default 1
        port = 9700  # default 9700

        def sort_chunks(self):
            """
            按chunk空闲程度对其排序
            :return: chunkno：list 按闲->忙排列
            """
            chunk_nos_ls = list(self.__class__.block_table.values())
            chunk_nos = [j for i in chunk_nos_ls for j in i]  # 二维列表转为一维 统计block_id
            chunk_times = Counter(chunk_nos).most_common()  # 统计每个chunk_no出现次数,并按出现次数降序排序
            chunk_nos_sort_ls = [pair[0] for pair in chunk_times]  # 提取出chun_no
            for chunk_no in self.__class__.chunks.keys():  # 如果部分chunk为空 则没有出现 因此这里补上
                if chunk_no not in chunk_nos_sort_ls:
                    chunk_nos_sort_ls.append(chunk_no)
            return chunk_nos_sort_ls[::-1]  # 按出现次数升序排序

        def exposed_allocate(self, size):
            """
            为指定大小的文件分配blocks
            :param size: 文件大小
            :return block_ids:分配的block_id 格式为[block_id1,,....]
            :return block_addresses:每个block对应chunk的地址 格式为[[(ip,port),..,],...]
            """
            block_ids = []
            block_addresses = []
            num_blocks = math.ceil(float(size) / self.__class__.block_size)
            for i in range(0, num_blocks):
                block_id = uuid.uuid1()  # 生成唯一标识符
                chunk_nos = self.sort_chunks()
                while len(chunk_nos) < self.__class__.replication:  # 冗余备份大于chunksever个数，因此需重复
                    chunk_nos += chunk_nos
                chunk_no_select = chunk_nos[:self.__class__.replication]  # 按空闲程度选取
                chunk_address = [self.__class__.chunks[i] for i in chunk_no_select]  # 根据chunk的编号，得到其地址
                self.__class__.block_table[block_id] = chunk_no_select  # 添加block和chunk映射关系
                block_ids.append(block_id)
                block_addresses.append(chunk_address)
            return block_ids, block_addresses

        def exposed_write(self, filename, block_ids):
            """
            将已经写入的文件记录下来
            :param filename: 存放文件名字
            :param block_ids: 存放文件对应的block id信息
            :return:
            """
            self.__class__.file_table[filename] = block_ids

        def exposed_exist(self, filename):
            """
            :param filename: 判断文件是否在文件系统中
            :return:
            """
            return filename in self.__class__.file_table

        def exposed_get_block_size(self):
            """
            获取block块大小
            :return block_size: int
            """
            return self.__class__.block_size

        def exposed_get_blocks(self, filename):
            """
            返回文件名所在的blocks及其地址
            :param filename: 删除文件名
            :return block_ids:需要删除的chunk_id
            :return block_addresses: 需要删除chunk在整个集群中服务器的位置
            """
            block_ids = self.__class__.file_table[filename]
            block_addresses = []
            for id in block_ids:
                chunk_nos = self.__class__.block_table[id]
                block_addresses.append([self.__class__.chunks[no] for no in chunk_nos])
            return block_ids,block_addresses

        def exposed_delete(self,filename,block_ids):
            """
            删除文件的元数据
            :param filename: 文件名
            :param block_ids: 文件对应的block
            :return:
            """
            for id in block_ids:
                self.__class__.block_table.pop(id)
            self.__class__.file_table.pop(filename)
            return

        def exposed_append(self, filename, block_ids):
            """
            将分配的block id追加到索引表中
            :param filename: 追加的文件名
            :param block_ids: 追加的block id
            :return:
            """
            self.__class__.file_table[filename] += block_ids

if __name__ == "__main__":
    init()
    signal.signal(signal.SIGINT, int_handler)
    t = ThreadedServer(MasterService, port=MasterService.exposed_Master.port)
    print(f"master启动成功,地址：localhost:{MasterService.exposed_Master.port}")
    t.start()
