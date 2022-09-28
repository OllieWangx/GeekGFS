import configparser
import random
import time

import rpyc
import os
import logging

logging.basicConfig(format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',  # 定义输出log的格式
                    datefmt='%Y-%m-%d %A %H:%M:%S',
                    level=logging.DEBUG)
LOG = logging.getLogger(__name__)


class Client():
    master = None

    def __init__(self):
        conf = configparser.ConfigParser()
        with open('gfs.init') as fp:
            conf.read_file(fp)
        port = eval(conf.get('master', 'port'))
        con = rpyc.connect("localhost", port=port)
        self.master = con.root.Master()
        LOG.info("主节点连接成功！")

    def read(self, filename: str, file_output=None):
        """
        读取文件
        :param file_output: 存储在本地的文件名,当为None时就使用filename
        :param filename: 文件名 str
        :return:
        """
        if not self.master.exist(filename):
            LOG.info("该文件不存在，删除失败")
            return
        block_ids, block_addresses = self.master.get_blocks(filename)
        file_output = filename if file_output is None else file_output
        with open(file_output, 'wb') as fp:
            for block_id, block_address in zip(block_ids, block_addresses):
                ip, port = random.sample(block_address, 1)[0]  # 随机选取一个地址
                con = rpyc.connect(ip, port=port)
                chunkServer = con.root.ChunkServer()
                data = chunkServer.read(block_id)
                fp.write(data)
        LOG.info("读出成功！")

    def write(self, oriFile: str, saveFile: str):
        """
        写入数据到chunk
        :param oriFile: 源文件 str
        :param saveFile: 目标文件 str
        :return:
        """
        if self.master.exist(oriFile):
            LOG.info("该文件已存在，写入失败")
            return
        size = os.path.getsize(oriFile)
        block_ids, block_addresses = self.master.allocate(size)
        block_size = self.master.get_block_size()
        with open(oriFile, 'rb') as fp:
            LOG.info("Writing: 【" + str(saveFile) + '】')
            for block_id, block_address in zip(block_ids, block_addresses):
                data = fp.read(block_size)
                ip, port = block_address[0]
                chunk_remain = block_address[1:]
                con = rpyc.connect(ip, port=port)
                chunkServer = con.root.ChunkServer()
                chunkServer.write(block_id, data, chunk_remain)
            self.master.write(saveFile, block_ids)
            LOG.info("写入成功")

    def exist(self, filename: str):
        """
        :param filename: 文件名
        :return:
        """
        if self.master.exist(filename):
            msg = "文件存在"
        else:
            msg = "文件不存在"
        LOG.info(msg)

    def delete(self, filename: str):
        """
        :param filename: 删除文件名
        :return:删除是否成功 boolean类型
        """
        if not self.master.exist(filename):
            LOG.info("该文件不存在，删除失败")
            return False
        block_ids, block_addresses = self.master.get_blocks(filename)
        # chunkserver删除存储数据
        for block_id, block_address in zip(block_ids, block_addresses):
            ip, port = block_address[0]
            chunk_remain = block_address[1:]
            con = rpyc.connect(ip, port=port)
            chunkServer = con.root.ChunkServer()
            chunkServer.delete(block_id, chunk_remain)
        # master删除元数据
        self.master.delete(filename, block_ids)
        LOG.info("文件删除成功")

    def append(self, filename_ori, filename_append):
        """
        :param filename_ori:追加源文件名字
        :param filename_append:追加新文件
        :return:
        """
        if not os.path.exists(filename_append):
            LOG.info("追加文件不存在，追加失败")
            return False
        if not self.master.exist(filename_ori):
            LOG.info("该源文件不存在，追加失败")
            return False
        block_ids, block_addresses = self.master.get_blocks(filename_ori)

        block_id_append = block_ids[-1]  # 只需处理最后一个block即可 因为是追加
        block_address_append = block_addresses[-1]
        size_append = os.path.getsize(filename_append)  # 追加文件的大小
        block_size = self.master.get_block_size()  # block的大小
        '''获取最后一个block的大小'''
        ip, port = block_address_append[0]  # 选取一个去获得大小即可 因为每个备份是一样的
        con = rpyc.connect(ip, port=port)
        chunkServer = con.root.ChunkServer()
        last_block_size = chunkServer.get_block_size(block_id_append)  # 最后一个block的大小

        # 往最后一个block写数据
        with open(filename_append, 'rb') as fp:
            remain_size = block_size - last_block_size  # 最后一个block剩余空间
            LOG.info("Appending: 【" + str(filename_append) + '】 TO 【' + str(filename_ori) + '】')
            data = fp.read(min(remain_size, size_append))
            chunkServer.write(block_id_append, data, block_address_append[1:], 'ab')

            if remain_size < size_append:  # 还需再分配block
                block_ids_a, block_addresses_a = self.master.allocate(size_append - remain_size)
                for block_id, block_address in zip(block_ids_a, block_addresses_a):
                    data = fp.read(block_size)
                    ip, port = block_address[0]
                    chunk_remain = block_address[1:]
                    con = rpyc.connect(ip, port=port)
                    chunkServer = con.root.ChunkServer()
                    chunkServer.write(block_id, data, chunk_remain)
                self.master.append(filename_ori, block_ids_a)
            LOG.info("追加成功")
        return True


if __name__ == "__main__":
    hint = """TIPS: 
    退出 exit()
    写   write:   put 本地文件名 存入名字
    读    read:   get 存入名字 本地保存名(可不写，建议写)
    存在 exist:   exist 文件名
    删除delete:   delete 文件名
    追加append:   append  源文件名 追加文件名
    """
    print(hint)
    client = Client()
    while True:
        time.sleep(0.5)
        input_str = input("【your command】:")
        str_split = input_str.split(" ")
        if str_split[0] == 'exit()':
            break
        elif str_split[0] == 'put' and len(str_split) == 3:
            client.write(str_split[1], str_split[2])
        elif str_split[0] == 'get' and len(str_split) == 3:
            client.read(str_split[1], str_split[2])
        elif str_split[0] == 'append' and len(str_split) == 3:
            client.append(str_split[1], str_split[2])
        elif str_split[0] == 'exist' and len(str_split) == 2:
            client.exist(str_split[1])
        elif str_split[0] == 'delete' and len(str_split) == 2:
            client.delete(str_split[1])
        else:
            print("指令错误")
