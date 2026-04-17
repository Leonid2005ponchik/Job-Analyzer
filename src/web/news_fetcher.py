import feedparser

def fetch_news(url, limit=None):
    try:
        news = []
        total_saved = 0 
        feed = feedparser.parse(url)

        if len(feed.entries) > 0:
            print(f"Найдено новостей: {len(feed.entries)}")
        else:
            print(f"Записей по новостям нет!")
        if feed.entries: 
            for item in feed.entries[:limit]:
                row_news = {
                    'title': item.get('title'),
                    'link': item.get('link'),
                    'published': item.get('published') or "Дата неизвестна", 
                    'summary': item.get('summary', item.get('description', ''))
                }

                total_saved += 1
                print(f"Запись о новостной публикации сохранена!. Количество сохранений: {total_saved}")
                news.append(row_news)

        return news 

    except Exception as e:
        print(f"Ошибка: {e}")
        return []

if __name__ == "__main__":
    fetch_news()