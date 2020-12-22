import os
import json
import time
from datetime import datetime
import re
import asyncio
import aiohttp
import aiofiles
import pandas as pd
from bs4 import BeautifulSoup

table = []
timeout_url = []

base_url = 'https://zure.fun'
url_format = base_url + '/book/%s/?page=%d'
download_url_format = base_url + '/book/down/epub/%s'
folder_path = 'download/'
pattern = r"[\/\\\:\*\"\|]"  # '/ \ : * " |'

# 获取网页（文本信息）
async def fetch(client, url):
    print(url)
    async with client.get(url) as response:
        assert response.status == 200
        return await response.text()


async def parser_desc(html):
    soup = BeautifulSoup(html, 'lxml')
    content = soup.select_one('h3')
    desc = []
    for elm in content.find_next_siblings():
        if elm.text == '作者简介':
            break
        desc.append(elm.text)
    tags = [tag.text.strip() for tag in soup.select('.mdui-chip')]
    return '\n'.join(desc), '\n'.join(tags)


async def parser_books(client, html, category):
    books = []
    tasks = []
    soup = BeautifulSoup(html, 'lxml')
    book_items = soup.select('.item')
    for book_item in book_items:
        book_id = book_item.a.get('href').replace('book', '').strip('/')
        title = book_item.select_one('.title').text
        author = book_item.select_one('.auths').text.strip().replace(' ', '')
        rank = book_item.select_one('.score > .number').text.replace('豆', '')
        download_book_url = download_url_format % book_id
        cover_url = book_item.select_one('.cover').img.get('src')
        file_path = "books/" + re.sub(pattern, "_", f"{title}.epub")  # 替换为下划线
        cover_path = "covers/" + os.path.basename(cover_url)
        if not os.path.exists(folder_path + file_path):
            tasks.append(asyncio.create_task(
                download(client,  download_book_url, folder_path + file_path)))
        if not os.path.exists(folder_path + cover_path):
            tasks.append(asyncio.create_task(
                download(client, cover_url, folder_path + cover_path)))
        book_url = base_url + book_item.a.get('href')
        book_detail = await fetch(client, book_url)
        desc, tags= await parser_desc(book_detail)
        created_at = updated_at = datetime.utcnow()
        table.append((book_id, title, author, category, desc, rank, file_path, cover_path, tags, created_at, updated_at))
    await asyncio.wait(tasks)


# 抓取图书
async def scrapy_books(client, url, category):
    try:
        content = await fetch(client, url)
        books_html = json.loads(content)['content']
        await parser_books(client, books_html, category)
    except:
        pass

# 下载文件
async def download(client, url, save_path):
    try:
        print('file: ' + save_path + ' to do!')
        async with client.get(url, timeout=600) as response:
            assert response.status == 200
            file = await response.read()    # 以Bytes方式读入非文字
            async with aiofiles.open(save_path, 'wb') as f:  # 写入文件
                await f.write(file)
                print('file: ' + save_path + ' done!')
    except Exception as e:
        timeout_url.append((url, save_path))
        print('download file: ' + save_path + ' timeout!')
        print('download url: ' + url + ' timeout!')
    

categorys = [('category1', '小说文学'), ('category2', '人文社科'), ('category3', '经济管理'), ('category4', '历史传记'), 
             ('category5', '学习教育'), ('category6', '励志成功'), ('category8', '生活时尚'), ('category9', '外文原版')]

# 处理网页
async def main():
    async with aiohttp.ClientSession() as client:
       tasks = []
       for category_url, category in categorys:
           for i in range(10):
               tasks.append(asyncio.create_task(scrapy_books(client, url_format % (category_url, i+1), category)))
       await asyncio.wait(tasks)

def process_download():
    global table
    res = []
    for row in table:
        book_path = folder_path + row[6]
        cover_path = folder_path + row[7]
        if not os.path.exists(book_path):
            if os.path.exists(cover_path):
                os.remove(cover_path)
        else:
            res.append(row)
    table = res

    book_folder = 'download/books/'
    names = os.listdir(book_folder)
    for name in names:
        for row in table:
            if row[6] == 'books/' + name:
                break
        else:
            book_path = book_folder + name
            if os.path.exists(book_path):
                os.remove(book_path)


if __name__ == "__main__":
    book_folder_path = os.path.join(folder_path, 'books')
    cover_folder_path = os.path.join(folder_path, 'covers')
    if not os.path.exists(book_folder_path):
        os.makedirs(book_folder_path)
    if not os.path.exists(cover_folder_path):
        os.makedirs(cover_folder_path)
    # 统计该爬虫的消耗时间
    print('#' * 50)
    t1 = time.time() # 开始时间

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    print('#' * 50)
    # 处理下载的电子书
    print('处理下载的电子书')
    process_download()

    # print(table)
    # 将table转化为pandas中的DataFrame并保存为CSV格式的文件
    df = pd.DataFrame(table, columns=['book_id','title','author','category','desc','rank', 'file_path', 'cover_path', 'tags', 'created_at', 'updated_at']) 
    df.to_csv('books.csv', index=False, encoding='utf_8')

    t2 = time.time() # 结束时间
    print('使用aiohttp，总共耗时：%s' % (t2 - t1))
    print('使用aiohttp，获取记录：%s' % len(table)) 
    print('#' * 50)
    print(timeout_url)
    print('下载超时的文件记录：%s' % len(timeout_url))
    print('#' * 50)
