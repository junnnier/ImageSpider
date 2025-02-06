import argparse
import sys
import time
import logging
import requests
import os
import hashlib
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import Edge
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
requests.packages.urllib3.disable_warnings()


class CustomError(Exception):
    def __init__(self, message):
        super(CustomError).__init__()
        self.message = message

    def __str__(self):
        return f"[Stop]: {self.message}"


def setup_logger(log_dir=None, filename=""):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # 日志输出到文件
    if log_dir:
        log_path = os.path.join(log_dir, filename) if filename else os.path.join(log_dir, "logs_{}.txt".format(time.strftime("%Y%m%d_%H%M%S",time.localtime())))
        file_handler = logging.FileHandler(filename=log_path)
        file_handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d [%(levelname)s] - %(message)s", datefmt='%Y-%m-%d %H:%M:%S'))
        root_logger.addHandler(file_handler)
    # 日志输出到屏幕
    stream_handler = logging.StreamHandler(sys.stdout)
    root_logger.addHandler(stream_handler)
    return root_logger


def load_keywords_data(file_path, logger):
    keywords = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            keywords.append(line.strip())
    logger.info("The number of keywords: {}\n{}".format(len(keywords), keywords))
    return keywords


def multi_thread(param):
    logging.info("Multi Thread starting.")
    with ThreadPoolExecutor(max_workers=4) as executor:  # 创建一个最多10个线程的线程池
        # 使用executor.submit提交任务到线程池
        futures = [executor.submit(crawl, *item) for item in param]
        # 等待所有任务完成，如果需要处理异常，可以在这里添加对futures的遍历和异常检查
        concurrent.futures.wait(futures,timeout=5)
    logging.info("Multi Thread ending.")


class image_loaded(object):
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        img_element = element.find_element(By.TAG_NAME, "img")  # 第一个<img>标签
        src = img_element.get_attribute("src")
        style = img_element.get_attribute("style")
        if "http" in src and "max-width" in style:
            return src
        else:
            return False


def save_img(filename, r):
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def download_image(url, save):
    agent = {
        "Connection": "close",
        "authority": "encrypted - tbn0.gstatic.com",
        "method": "GET",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=0, i",
        "sec-ch-ua": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-arch": "x86",
        "sec-ch-ua-bitness": "64",
        "sec-ch-ua-full-version-list": '"Microsoft Edge";v="131.0.2903.146", "Chromium";v="131.0.6778.265", "Not_A Brand";v="24.0.0.0"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": "",
        "sec-ch-ua-platform": "Windows",
        "sec-ch-ua-platform-version": "19.0.0",
        "sec-ch-ua-wow64": "?0",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    r = requests.get(url, headers=agent, stream=True, verify=False)
    try:
        if r.status_code == 200:
            md5value = hashlib.md5(r.content).hexdigest()
            file_name = md5value + ".jpg"
            file_path = os.path.join(save, file_name)
            # 保存
            save_img(file_path, r)
            r.close()
            return True, file_path
    except:
        return False, r.status_code


def crawl(keywords, save_root_dir, download_number, logger):
    # 是否创建存储目录
    save_dir = os.path.join(save_root_dir, keywords)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        logger.info("create directory {}".format(save_dir))
    # 初始化
    wd = Edge(service=Service('msedgedriver.exe'))  # 启动浏览器驱动，运行浏览器
    wd.set_page_load_timeout(100)  # 页面加载最大超时
    wd.maximize_window()  # 最大窗口
    url = f"https://www.google.com/search?q={keywords}&tbm=isch&hl=zh-CN&tbs=qdr:&sa=X&ved=0CAIQpwVqFwoTCOiMx5-F04YDFQAAAAAdAAAAABAC&biw=1217&bih=630"
    wd.get(url=url)
    time.sleep(3)

    image_count = 0  # 图片数量
    roll_bar_position = 0  # 滚动条初始位置
    crawed_elements = set()
    while True:
        try:
            # 滑动滚动条
            page_height = wd.execute_script("return document.body.scrollHeight")  # 获取滚动条的长度
            wd.execute_script("window.scrollBy(0, {});".format(roll_bar_position))
            roll_bar_position += 1000
            # 位置达到滚动条的最大值，尝试点击加载更多的图片
            if roll_bar_position >= page_height:
                try:
                    more_element = wd.find_element(By.XPATH, '//span[@class="RVQdVd"]')
                    if more_element:
                        logging.info("finding more image element...")
                        more_element.click()  # 如果找到，点击按钮
                        time.sleep(3)  # 等待加载
                except NoSuchElementException:
                    # 如果没有找到元素，继续循环等待加载
                    logging.info("not find more image elements.")
            time.sleep(1.0)
            # 找到页面中的所有图片元素
            img_elements = WebDriverWait(wd, 20).until(
                EC.presence_of_all_elements_located((By.XPATH, "//div[@jsname='dTDiAc']/div[@jsname='qQjpJ']/h3/a"))
            )
            # 网页内去重
            for index_, item in enumerate(img_elements):
                if item in crawed_elements:
                    img_elements.pop(index_)
            elements_num = len(img_elements)
            # 新增图片的数量
            logger.info("find new image element: {}".format(elements_num))
            # 对每张图片元素操作
            for index, img_element in enumerate(img_elements):
                # 点开大图页面
                try:
                    img_element.click()
                except:
                    logger.info("find [{}/{}]\t{}\t{}".format(index + 1, elements_num, keywords, "click error, next image."))
                    continue
                # 等待大图页面加载完成
                locator = (By.XPATH, '//div[@jsname="CGzTgf"]//div[@jsname="figiqf"]/a')
                src = WebDriverWait(wd, 5).until(image_loaded(locator))
                logger.info("find [{}/{}]\t{}\t{}".format(index + 1, elements_num, keywords, src))
                try:
                    if src.startswith('https'):
                        # 下载
                        result, info = download_image(src, save_dir)
                        if result:
                            logger.info("download success, save to {}".format(info))
                            image_count += 1
                        else:
                            logger.info("download failure, http code {}, {}".format(info, src))
                            continue
                except:
                    logger.info("big image loading failure.")
                    continue
                # 记录抓取过的这批图
                crawed_elements = crawed_elements.union(set(img_elements))
                # 回退
                logging.info("go back.")
                wd.back()
                time.sleep(0.1)
                # 如果设置下载量且达到数量
                if download_number and image_count >= download_number:
                    raise CustomError("The number of images has reached.")
        except CustomError as e:
            logger.info(e)
            break
        except:
            logger.info("The scroll bar is already at the end.")
            break


def main(opt):
    config_path = opt.file
    save_root_dir = opt.save
    log_dir = opt.log
    multi = opt.multi
    download_number = opt.num

    # 创建日志记录
    logger = setup_logger(log_dir=log_dir)
    # 加载关键词文件
    keywords_list = load_keywords_data(config_path, logger)
    # 是否多线程执行
    if multi:
        param_list = [[keywords, save_root_dir, download_number, logger] for keywords in keywords_list]
        multi_thread(param_list)
    else:
        # 对每个关键词操作
        for keywords in keywords_list:
            # 爬取
            start_time = time.time()
            crawl(keywords, save_root_dir, download_number, logger)
            end_time = time.time()
            logging.info("{} total time:{}".format(keywords, end_time - start_time))


def parse_opt():
    parser = argparse.ArgumentParser(description="Google Image Spider")
    parser.add_argument('--file', type=str, default='./conf/test.txt', help='The path of keywords file.')
    parser.add_argument('--save', type=str, default='./download_file', help='The path of save directory. If not exist, auto created it.')
    parser.add_argument('--log', type=str, default='./', help='The path of directory for save log file.')
    parser.add_argument('--num', type=int, default=0, help='The number of images to download. default all images.')
    parser.add_argument('--multi', action="store_true", help='multi-thread download.')
    opt = parser.parse_args()
    return opt


if __name__ == '__main__':
    opt = parse_opt()
    main(opt)
    print("end")