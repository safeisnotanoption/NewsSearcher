import lxml.html
import lxml.html.soupparser
import lxml.etree
from tqdm import tqdm
from aiohttp import ClientSession
import itertools as it
import asyncio
import urllib.parse
import sqlite3


db = sqlite3.connect('Database.db')
db.execute(
    "CREATE TABLE IF NOT EXISTS fontanka (id INTEGER PRIMARY KEY UNIQUE NOT NULL, title TEXT, content TEXT)")


async def get_page(url, session):
    try:
        async with session.get(url) as response:
            return await response.read()
    except Exception:
        return '<html></html>'


async def get_pages(urls):
    tasks = []
    async with ClientSession() as session:
        for url in urls:
            task = asyncio.ensure_future(get_page(url, session))
            tasks.append(task)
        return await asyncio.gather(*tasks)


async def grab():
    years_urls = list(map(lambda y: 'http://www.fontanka.ru/fontanka/arc/' + str(y) + '/all.html', (2015, 2017)))
    years_pages = map(lxml.html.fromstring, await get_pages(years_urls))
    days_urls = list(it.chain.from_iterable(
        map(lambda y: y.xpath('//table[contains(@class, \'blank_year\')]//a/@href'), years_pages)))
    days_urls = list(map(lambda u: urllib.parse.urljoin('http://www.fontanka.ru/', u), days_urls))
    del years_pages
    days_pages = map(lxml.html.fromstring, await get_pages(days_urls))
    news_urls = list(it.chain.from_iterable(
        map(lambda y: y.xpath('//div[contains(@class, \'calendar-item-title\')]/a/@href'), days_pages)))
    news_urls = list(map(lambda u: urllib.parse.urljoin('http://www.fontanka.ru/', u), news_urls))
    del days_pages
    k = len(news_urls) // 1000
    for j in tqdm(range(k)):
        news_pages = list(map(lxml.html.fromstring, await get_pages(news_urls[j * 1000:(j + 1) * 1000])))
        titles = map(lambda p: p.xpath('normalize-space(//div[contains(@class, \'article_title\')])'), news_pages)
        article_introtext = map(lambda p: p.xpath('normalize-space(//div[contains(@class, \'article_introtext\')])'),
                                news_pages)
        article_fulltext = map(lambda p: p.xpath('normalize-space(//div[contains(@class, \'article_fulltext\')])'),
                               news_pages)
        contents = [' '.join(a) for a in zip(article_introtext, article_fulltext)]
        # tags = map(lambda p: p.xpath('normalize-space(//div[contains(@class, \'article_cat\')])'), news_pages)
        db.executemany('INSERT INTO fontanka (title, content) VALUES (?, ?)', zip(titles, contents))
        db.commit()


def clear_the_database():
    db.execute('DELETE FROM fontanka WHERE trim(title) = "" OR trim(content) = ""')
    db.execute('DELETE FROM fontanka WHERE title IS NULL')
    db.execute('DELETE FROM fontanka WHERE content IS NULL')
    db.commit()


print("Download start")
asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(grab()))
print("Download complete")
print("Clean start")
clear_the_database()
print("Clean complete")
# print("Tokenization start")
# rows = db.execute('SELECT article, category FROM fontanka').fetchall()
# i = 0
# db.execute("CREATE TABLE IF NOT EXISTS cleanka (id INTEGER PRIMARY KEY UNIQUE NOT NULL, article TEXT, category TEXT)")
# for i in tqdm(range(len(rows) // 1000)):
#     start = i * 1000
#     end = (i + 1) * 1000
#     x = list(map(lambda z: z[0], rows))[start:end]
#     y = list(map(lambda z: z[1], rows))[start:end]
#     for j in range(len(x)):
#         t = RegexpTokenizer(r'\w+').tokenize(x[j])
#         t = map(lambda z: z.lower(), t)
#         t = filter(lambda z: z not in stopwords.words('russian'), t)
#         t = filter(lambda z: len(z) > 2, t)
#         t = map(SnowballStemmer('russian').stem, t)
#         t = map(lambda z: '#' if z.isdigit() else z, t)
#         x[j] = ' '.join(t)
#     db.executemany('INSERT INTO cleanka (article, category) VALUES (?, ?)', zip(x, y))
#     db.commit()
# del rows
# print("Tokenization complete")
# print("Pickle start")
# rows = db.execute('SELECT article, category FROM cleanka')
# data = rows.fetchall()
# x = list(map(lambda z: z[0], data))
# y = list(map(lambda z: z[1], data))
# y = numpy.array(y)
# db.close()
# with open('Data_X.pickle', 'wb') as f:
#     pickle.dump(x, f)
# with open('Data_Y.pickle', 'wb') as f:
#     pickle.dump(y, f)
# print("Pickle complete")
