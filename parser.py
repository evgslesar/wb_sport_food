import requests
import json
import time
import sqlite3
import pandas as pd


def get_json(url):
    data = []
    for page in range(1, 8):
        querystring = {"spp":"0",
            "regions":"64,83,4,38,33,70,82,75,30,69,86,40,22,1,31,66,48,71,80,68","stores":"117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043",
            "pricemarginCoeff":"1.0","reg":"0","appType":"1","offlineBonus":"0",
            "onlineBonus":"0","emp":"0","locale":"ru","lang":"ru","curr":"rub",
            "couponsGeo":"12,3,18,15,21",
            "dest":"-1029256,-102269,-1278703,-1255563","supplier":"16584",
            "sort":"popular","page":f"{page}"}

        payload = ""
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7,de;q=0.6",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Origin": "https://www.wildberries.ru",
            "Pragma": "no-cache",
            "Referer": "https://www.wildberries.ru/seller/16584?sort=popular&page=7",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
            "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"'
        }

        response = requests.request("GET", url=url, data=payload, headers=headers, params=querystring)

        page_data = response.json()
        data.extend(page_data.get('data').get('products'))
        time.sleep(2)

    # with open('wb_seller.json', 'w', encoding='utf-8') as f:
    #     json.dump(data, f, ensure_ascii=False)

    return data


def get_offers(data):
    articles = []
    for item in data:
        id = item["id"]
        brand = item["brand"]
        name = item["name"]
        full_price = item["priceU"]/100.00
        discount = item["sale"]
        price = item["salePriceU"]/100.00
        rating = item["rating"]
        feedbacks_num = item["feedbacks"]
        pics_num = item["pics"]
        pics_urls = [f'https://images.wbstatic.net/c516x688/new/{(id//10000)*10000}/{id}-{i}.jpg' 
                        for i in range(1, pics_num+1)]
        article_url = f'https://www.wildberries.ru/catalog/{id}/detail.aspx'
        
        res_dict = {
            'id': id,
            'brand': brand,
            'name': name,
            'full_price': full_price,
            'discount': discount,
            'discounted_price': price,
            'rating': rating,
            'feedbacks_num': feedbacks_num,
            'pictures_num': pics_num,
            'pictures_urls': ',\n'.join(pics_urls),
            'article_url': article_url
        }
        articles.append(res_dict)
    
    return articles
    

def save_to_db(articles):
    df = pd.DataFrame(articles)
    conn = sqlite3.connect('wb_sport_food.db')
    df.to_sql("wb_sport_food_info", conn)
    conn.close()


if __name__ == '__main__':
    url = "https://wbxcatalog-ru.wildberries.ru/sellers/catalog"
    data = get_json(url)
    # with open('wb_seller.json', 'r', encoding='utf-8') as f:
    #     data = json.load(f)

    articles = get_offers(data)
    save_to_db(articles)