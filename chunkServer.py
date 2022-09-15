import sys
import rpyc
import os
from rpyc.utils.server import ThreadedServer


class ChunkService(rpyc.Service):
    class exposed_ChunkServer():
        def exposed_write(self, block_id, data, chunks,mode='wb'):
            """
            将数据写入本chunk并将数据发给其他chunk
            :param mode: 写入模式 w为写入 a为追加
            :param block_id: block的唯一标识符
            :param data: 需要写入数据
            :param chunks: 需要发送的chunk list
            :return:
            """
            with open(DATA_DIR + str(block_id), mode) as f:
                f.write(data)
            if len(chunks) > 0: #向其他chunk发送数据
                ip, port = chunks[0]
                chunks = chunks[1:]
                con = rpyc.connect(ip, port=port)
                chunkServer = con.root.ChunkServer()
                chunkServer.write(block_id, data, chunks,mode)

        def exposed_read(self, block_id):
            """
            :param block_id:需要读取的block_id
            :return:
            """
            block_addr = DATA_DIR + str(block_id)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr,'rb') as fp:
                return fp.read()

        def exposed_delete(self, block_id,chunks):
            """
            :param block_id: 删除文件的id
            :param chunks: 余下需删除chunk的位置
            :return:
            """
            block_addr = DATA_DIR + str(block_id)
            os.remove(block_addr)
            print(f'Deleting file【{block_id}】')
            if len(chunks) > 0: #其他chunk删除
                ip, port = chunks[0]
                chunks = chunks[1:]
                con = rpyc.connect(ip, port=port)
                chunkServer = con.root.ChunkServer()
                chunkServer.delete(block_id, chunks)

        def exposed_get_block_size(self,block_id):
            """
            :param block_id: 获取指定block的大小
            :return:该block的大小
            """
            return os.path.getsize(DATA_DIR + str(block_id))

if __name__ == "__main__":
    port = sys.argv[1]

    DATA_DIR = "./chunk/localhost-" + port + "/"
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    chunk = ThreadedServer(ChunkService, port=eval(port))
    print(f"chunkServer启动成功,地址：localhost:{port}")
    chunk.start()
