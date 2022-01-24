"""
爬取知乎问答图片
2021/12/18
"""
# TODO https://www.zhihu.com/search?type=content&q=关键词
# TODO 改进程序架构，不要写成类的形式，不然太复杂了


import re
import requests
import os
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED


# logging.basicConfig(filename='logger.log', level=logging.INFO)
logging.basicConfig(filename='logger.log', level=logging.INFO)

# logging.debug('debug message')
# logging.info('info message')
# logging.warn('warn message')
# logging.error('error message')
# logging.critical('critical message')

def list_del_overlap(lst: list) -> list:
    """删除列表中重复的元素"""
    return list(set(lst))


class ZhihuQuestion():
    """知乎问题爬虫"""

    def __init__(self, id, data_base_dir='./data', ans_num_limit=-1, overwrite=False) -> None:
        """初始化
        id: 问题的id
        data_base_dir: 数据文件存放的基目录（默认：'./data'）
        ans_num_limit: 限制采集的最大数量（默认：-1 无限制）
        overwrite: 是否允许覆盖已有文件（默认：False 不允许）
        """
        self.id = id
        self.ans_num_limit = ans_num_limit
        self.overwrite = overwrite  # 数据文件是否允许覆盖
        # 目录结构：
        # - data_base_dir/
        #   - <self.id>/
        #       - <self.id>.html
        #       - <self.id.json>
        #       - images/
        #           - <ans1_id>/
        #               - <pic1_name>.jpg
        #               - <pic2_name>.jpg
        #               - ...
        #           - <ans2_id>/
        #           - ...
        self.__data_dir = os.path.join(data_base_dir, id)  # 存储html、json和图片等数据的文件夹路径
        self.__html_path = os.path.join(self.__data_dir, id+'.html')
        self.__json_path = os.path.join(self.__data_dir, id+'.json')
        self.__img_dir = os.path.join(self.__data_dir, 'images')
        self.url = {
            "question": f"https://www.zhihu.com/question/{id}",
            "answer": f"https://www.zhihu.com/api/v4/questions/{id}/answers"}
        if not os.path.exists(self.__data_dir):
            os.makedirs(self.__data_dir)

        logging.info(f"开始爬取问题{self.id}...")
        # 1.爬取问题的HTML数据
        self.get_html()
        # 2.从HTML文件中获取回答总数
        with open(self.__html_path, mode='r', encoding='utf-8') as f:
            f_str = f.read()
        re_obj_1 = re.compile(r'<meta itemProp="answerCount" content="(\d+)"/>', re.S)
        match = re_obj_1.search(f_str)   # BUG DONE 如果不用compile的话，分号要转义！
        if match is None:
            logging.critical(f"{self.id}.html中匹配不到回答总数，程序退出")
            exit(-1)
        else:
            self.ans_count = int(match.group(1))
            logging.debug(f"问题{self.id}下回答数为：{self.ans_count}")
        # 3.从HTML文件中获取问题标题
        re_obj_2 = re.compile(r'<title .*?>(.*?) - 知乎</title>', re.S)
        match = re_obj_2.search(f_str)
        if match is None:
            logging.error(f"{self.id}.html中匹配不到问题标题")
        else:
            self.title = match.group(1)
            logging.debug(f"问题{self.id}的标题为：{self.title}")
        # 4.爬取回答的JSON数据
        self.get_json()
        logging.info(f"问题{self.id}爬取完毕")


    def get_html(self): # 为了获取总回答数、问题标题等
        """获取问题页的HTML文件"""
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36",
            "referer": f"https://www.zhihu.com/question/{self.id}"}

        if os.path.exists(self.__html_path) and self.overwrite==False:
            logging.info(f"{self.id}.html已存在，跳过HTML采集")
            return

        # HTML文件不存在，或允许文件覆盖
        logging.info(f"正在采集{self.id}.html...")
        with requests.get(self.url['question'], headers=headers) as respose:
            with open(self.__html_path, mode='wb') as f:
                f.write(respose.content)
        logging.info(f"{self.id}.html采集完成")


    def get_json(self):
        """获取所有回答的JSON数据"""
        re_obj = re.compile(r' *#.*')   # 去掉注释
        with open('./include.txt', 'r', encoding='utf-8') as f:
            include = re_obj.sub('', f.read())
            include = ''.join(include.split('\n'))

        params = {# 要爬取的数据域
            "include": include,
            "platform": "desktop",
            "sort_by": "default",   # created
            "limit": 20,    # 每次最多20个回答
            "offset": 0}
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36",
            "referer": f"https://www.zhihu.com/question/{self.id}"}

        if os.path.exists(self.__json_path) and self.overwrite==False:
            logging.info(f"{self.id}.json已存在，跳过jSON采集")
            return

        # 获取回答数据
        logging.info(f"正在采集{self.id}.json...")
        if self.ans_num_limit == -1:
            max_num = self.ans_count
        else:
            max_num = min(self.ans_num_limit, self.ans_count)

        offset = params['offset']
        limit = params['limit']
        data = []   # TODO 优化内存占用问题
        fs = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            while True:
                if offset + limit > max_num:    # 最后一次爬取不够limit个回答数据
                    limit = max_num - offset

                if offset == max_num: # 终止条件
                    break
                elif offset > max_num:
                    logging.critical("致命错误！程序退出。")
                    exit(-1)

                # 多线程爬取回答数据
                thread_name = str(offset+1)+'~'+str(limit)
                fs.append(executor.submit(self.get_ans_data(offset, limit, headers, params, data), (thread_name)))
                # 状态更新
                offset += limit
                params['offset'] = offset   # BUG DONE 这里忘记更新了，debug了好久。。。
                params['limit'] = limit

            wait(fs, return_when=ALL_COMPLETED)

        # 保存数据
        with open(self.__json_path, mode='w') as f:
            f.write(json.dumps(data))
        logging.info(f"{self.id}.json采集完成")


    def get_ans_data(self, offset, limit, headers, params, data) -> None:
        """爬取指定范围的回答数据"""
        logging.debug(f"正在爬取回答{offset+1}到{offset+limit}...")
        try:
            with requests.get(self.url['answer'], headers=headers, params=params) as respose:
                status_code = respose.status_code
                if status_code == 200:
                    data.extend(respose.json()['data'])
                else:
                    logging.error(f"出错，状态码为{status_code}")
        except Exception:
            logging.warn(f"回答{offset+1}到{offset+limit}数据采集失败")
        else:
            logging.debug(f"回答{offset+1}到{offset+limit}数据采集成功")


    def get_images(self):
        """获取问题下的所有图片"""
        num_of_ans_with_pics = 0
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36",
            "referer": f"https://www.zhihu.com/question/{self.id}"}

        if not os.path.exists(self.__img_dir):
            os.makedirs(self.__img_dir)

        logging.info(f"正在采集问题{self.id}中的图片...")
        with open(self.__json_path, mode='r', encoding='utf-8') as f:
            answers = json.load(f) # 数组

        logging.info(f"共有{len(answers)}个问题需要采集图片")
        for i, answer in enumerate(answers):
            img_urls = re.findall(r'<img src=.*?data-original="(.*?)"', answer['content'], re.S)
            if len(img_urls) == 0:    # 回答中没有图片
                logging.info(f"第{i}个回答{answer['id']}中没有图片，跳过")
                continue

            img_urls = list_del_overlap(img_urls)   # 去重。BUG JSON中有重复的项 findall是返回不重叠的，为什么会出现重叠的？

            # 回答中有图片
            num_of_ans_with_pics += 1
            logging.info(f"正在爬取第{i}个回答{answer['id']}的图片...")
            img_dir = os.path.join(self.__img_dir, str(answer['id']))
            if not os.path.exists(img_dir):
                os.makedirs(img_dir)    # 有图片的回答才建立文件夹，同一个回答的图片放一个文件夹下

            img_paths = []
            for img_url in img_urls:
                img_name = img_url.split('/')[-1].split('?')[0]
                img_paths.append(os.path.join(img_dir, img_name))

            # 多线程爬取一个回答下的多张图片
            with ThreadPoolExecutor(max_workers=4) as executor:
                all_task = [executor.submit(get_data(img_url, img_path, headers), (img_url)) for img_url, img_path in zip(img_urls, img_paths)]
                wait(all_task, return_when=ALL_COMPLETED)

            logging.info(f"第{i}个回答{answer['id']}中的所有图片采集完毕")
            time.sleep(0.5)

        logging.info(f"问题{self.id}中的图片爬取完毕")
        logging.info(f"共爬取了{num_of_ans_with_pics}个回答的图片")


def get_data(url, path, headers, overwrite=False):
    """获取数据并写入文件"""
    if os.path.exists(path) and overwrite == False:    # 图片已存在
        logging.warning(f'{path}已经存在')
    else:
        try:
            with requests.get(url, headers=headers) as res:
                with open(path, mode='wb') as f:
                    f.write(res.content)
        except Exception as e:
            logging.error(e)
            logging.error(f'{url}下载失败')
        else:
            logging.debug(f'{url}下载完成，路径为{path}')



if __name__ == "__main__":

    logging.info(f"======================================执行时间{time.time()}================================================")
    question_ids = ['123456789', '111111111']  # 要爬取的知乎问题的id列表


    questions = []
    for question_id in question_ids:
        questions.append(ZhihuQuestion(question_id, ans_num_limit=-1)) # TODO 当偏移量不为0时，获取的回答数比ans_num_limit小

    for question in questions:
        question.get_images()



