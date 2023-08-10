import threading
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup, SoupStrainer
import sqlite3
from urllib import robotparser

# Установите соединение с базой данных
conn = sqlite3.connect('database.db', check_same_thread=False)
c = conn.cursor()
c.execute("CREATE TABLE IF NOT EXISTS documents (url TEXT PRIMARY KEY, title INTEGER , description INTEGER)")
# Создайте таблицу для хранения рейтинга
c.execute("CREATE TABLE IF NOT EXISTS ratings (url TEXT PRIMARY KEY, rating INTEGER)")

# Сохраняем изменения и закрываем соединение
conn.commit()

# Флаг для индексации
indexing = False
links = ['https://youtube.com','https://ru.wikipedia.org/wiki/Вторая_мировая_война', 'https://wikipedia.org' , 'https://google.com' , 'https://apple.com' , 'https://vc.ru/flood/170561-chto-delat-esli-skuchno-500-ssylok-sobrannyh-za-polgoda' , 'https://www.reddit.com' , 'https://vk.ru' , 'https://habr.com/ru/all/' , 'https://dzen.ru/news?issue_tld=ru&utm_referer=yandex.ru' , 'https://news.ycombinator.com' , 'https://news.mail.ru/']
import re
def crawl_site(site):
    try:
        # Проверяем наличие SSL и валидность сертификата
        response = requests.get(site, verify=True)
        if response.ok:
            response.encoding = "utf-8"
            # Загружаем содержимое сайта
            soup = BeautifulSoup(response.text, 'html.parser')
            # Извлекаем заголовок и контент
            title = soup.title.string if soup.title else ''
            # Ищем мета-тег с атрибутом name="description" или property="og:description"
            # Извлекаем содержимое страницы
            content = soup.get_text()

            # Удаляем лишние символы и пробелы из содержимого
            content = re.sub(r'\s+', ' ', content).strip()
            # Записываем данные в базу данных, игнорируя дублирующиеся записи
            c.execute("INSERT OR IGNORE INTO documents (url, title, description) VALUES (?, ?, ?)", (site, title, ''))

            # Ищем ссылки на сайте
            global links
            links += [urljoin(site, link.get('href')) for link in soup.find_all('a')]

            # Создаем объект RobotFileParser
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(site, "/robots.txt"))
            rp.read()

            # Бесконечный поиск индексации новых ссылок
            while len(links) > 0:
                link = links.pop(0)

                # Проверяем, разрешено ли индексирование для связанного сайта
                if rp.can_fetch("*", link):
                    # Проверяем, был ли сайт уже проиндексирован
                    c.execute("SELECT * FROM documents WHERE url=?", (link,))
                    result = c.fetchone()

                    if not result:
                        try:
                            # Проверяем наличие SSL и валидность сертификата для связанного сайта
                            response = requests.get(link, verify=True)

                            if response.ok:
                                # Загружаем содержимое связанного сайта
                                linked_soup = BeautifulSoup(response.text, 'html.parser')

                                # Извлекаем заголовок и контент
                                linked_title = linked_soup.title.string if linked_soup.title else ''
                                linked_content = linked_soup.get_text()

                                # Сокращаем описание сайта до двух предложений
                                sentences = linked_content.split('.')
                                linked_description = '. '.join(sentences[:2]) + '.'

                                # Записываем данные связанного сайта в базу данных
                                c.execute("INSERT INTO documents (url, title, description) VALUES (?, ?, ?)", (link, linked_title, linked_description))
                                conn.commit()

                                # Ищем новые ссылки на связанном сайте
                                linked_links = [urljoin(str(link), link.get('href')) for link in linked_soup.find_all('a')]
                                links += linked_links
                        except requests.exceptions.RequestException:
                            print(f'Не удалось загрузить содержимое сайта: {link}')
                    else:
                        print(f'Сайт уже проиндексирован: {link}')
                else:
                    print(f'Индексирование запрещено для сайта: {link}')
        else:
            print(f'Сайт недоступен: {site}')
    except requests.exceptions.RequestException:
        print(f'Не удалось загрузить содержимое сайта: {site}')

def crawl():
    global indexing

    # Получаем список сайтов для индексации
    sites = ['https://youtube.com','https://ru.wikipedia.org/wiki/Вторая_мировая_война', 'https://wikipedia.org' , 'https://google.com' , 'https://apple.com' , 'https://www.reddit.com' , 'https://vc.ru/flood/170561-chto-delat-esli-skuchno-500-ssylok-sobrannyh-za-polgoda' , 'https://vk.ru' , 'https://habr.com/ru/all/' , 'https://dzen.ru/news?issue_tld=ru&utm_referer=yandex.ru' , 'https://news.ycombinator.com' , 'https://news.mail.ru/']

    for site in sites:
        crawl_site(site)

    indexing = False
    print('Индексация завершена')

    # Выводим рейтинг каждой страницы
    c.execute("SELECT * FROM ratings ORDER BY rating DESC")
    results = c.fetchall()
    for result in results:
        print(f'{result[0]} - {result[1]} ссылок')

# Запуск функции crawl() в отдельном потоке
def start_crawl():
    global indexing
    indexing = True
    crawl()

if __name__ == '__main__':
    # Запуск индексации в отдельном потоке
    threading.Thread(target=start_crawl).start()