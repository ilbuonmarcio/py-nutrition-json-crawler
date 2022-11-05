import requests
import time
import json
import os
from pprint import pprint
from pathlib import Path
from bs4 import BeautifulSoup


global_start = time.time()


def download_pages():
    url = "https://www.alimentinutrizione.it/tabelle-nutrizionali/ricerca-per-ordine-alfabetico"

    response = requests.get(url)
    text = response.text

    soup = BeautifulSoup(text, "html.parser")
    page_urls = [food['href'] for food in soup.find_all('a') if '/tabelle-nutrizionali/' in food['href'] and len(food['href']) == 28]

    # Downloading all pages and writing those to a file:
    Path("./pages/").mkdir(parents=True, exist_ok=True)

    i = 1
    for page_url in page_urls:
        start_time = time.time()
        code = page_url.split("/")[2] + '.html'
        response = requests.get("https://www.alimentinutrizione.it" + page_url)
        text = response.text
        
        with open("./pages/" + code, 'w', encoding="utf-8") as output_file:
            soup = BeautifulSoup(text, "html.parser")
            output_file.write(soup.prettify())
            
        end_time = time.time()
        
        print(f"Downloading [{i}/{len(page_urls)}] {code} in {round((end_time - start_time) * 1000, 2)}ms")
        i += 1
        
        
def parse_pages():
    Path("./pages/").mkdir(parents=True, exist_ok=True)
    Path("./exports/").mkdir(parents=True, exist_ok=True)
    
    i = 1
    filenames = os.listdir('./pages/')
    for filename in filenames:
        with open('./pages/' + filename, 'r', encoding="utf-8") as pagefile:
            content = "".join(pagefile.readlines())
            
        soup = BeautifulSoup(content, 'html.parser')
        body = soup.select('section.article-content')[0]
        
        top_content_rows = [elem for elem in body.select('#conttableft')[0].select('table tr')]
        
        data_name_it = ' '.join(top_content_rows[0].select('h1.article-title')[0].get_text().split())
        data_category = ' '.join(top_content_rows[1].select('td')[1].get_text().split())
        data_food_code = ' '.join(top_content_rows[2].select('td')[1].get_text().split())
        data_name_en = ' '.join(top_content_rows[3].select('td')[1].get_text().split())
        data_edible_percentage = ' '.join(top_content_rows[4].select('td')[1].get_text().split())
        data_portion_qty = ' '.join(top_content_rows[5].select('td')[1].get_text().replace('%', '').split())
        
        nutrients = soup.select('tr.corponutriente')
        n = []
        for nutrient in nutrients:
            nutrient_cols = nutrient.select('td')
            nutrient = {
                "name": ' '.join(nutrient_cols[0].get_text().split()),
                "value_100g": ' '.join(nutrient_cols[2].get_text().split()),
                "value_portion": ' '.join(nutrient_cols[5].get_text().split())
            }
            n.append(nutrient)
            
        minerals = soup.select('tr.corpominerali')
        m = []
        for mineral in minerals:
            mineral_cols = mineral.select('td')
            mineral = {
                "name": ' '.join(mineral_cols[0].get_text().split()),
                "value_100g": ' '.join(mineral_cols[2].get_text().split()),
                "value_portion": ' '.join(mineral_cols[5].get_text().split())
            }
            m.append(mineral)
            
        vitamins = soup.select('tr.corpovitamine')
        v = []
        for vitamins in vitamins:
            vitamins_cols = vitamins.select('td')
            vitamins = {
                "name": ' '.join(vitamins_cols[0].get_text().split()),
                "value_100g": ' '.join(vitamins_cols[2].get_text().split()),
                "value_portion": ' '.join(vitamins_cols[5].get_text().split())
            }
            v.append(vitamins)
            
        aminoacids = soup.select('tr.corpoaminoacidi')
        a = []
        for aminoacids in aminoacids:
            aminoacids_cols = aminoacids.select('td')
            aminoacids = {
                "name": ' '.join(aminoacids_cols[0].get_text().split()),
                "value_100g": ' '.join(aminoacids_cols[2].get_text().split()),
                "value_portion": ' '.join(aminoacids_cols[5].get_text().split())
            }
            a.append(aminoacids)
        
        
        data = {
            "id": i,
            "name_it": data_name_it,
            "name_en": data_name_en,
            "category": data_category,
            "food_code": data_food_code,
            "edible_percentage": data_edible_percentage,
            "portion_qty": data_portion_qty,
            "nutrients": n,
            "minerals": m,
            "vitamins": v,
            "aminoacids": a
        }
        
        with open("./exports/" + filename.replace('html', 'json'), 'w', encoding="utf-8") as output_file:
            output_file.write(json.dumps(data, indent=4))
        
        print(f"Extracting [{i}/{len(filenames)}] {data['name_it']}")
         
        i += 1
        
        
def generate_sql_inserts():
    sql_inserts = []
        
    for filename in os.listdir('./exports/'):
        with open('./exports/' + filename, 'r') as input_file:
            food = json.loads(input_file.read())
            
            columns = []
            for key, value in food.items():
                if isinstance(value, list):
                    for elem in value:                        
                        # Cleaning name
                        column_name = elem['name'].replace('(g)', '') \
                                                .replace('(kcal)', '') \
                                                .replace('(kJ)', '') \
                                                .replace('(mg)', '') \
                                                .replace('(μg)', '') \
                                                .strip().lower().replace(' ', '_')
                        column_value = elem['value_100g']
                        columns.append([column_name, column_value])
                else:
                    columns.append([key, value])
                    
            keys = ", ".join([str(elem[0]) for elem in columns])
            values = ", ".join(['"' + str(elem[1]) + '"' for elem in columns])
            sql = f"INSERT INTO foods ({keys}) VALUES ({values});\n"
            
            sql_inserts.append(sql)
    
    with open("insert.sql", 'w', encoding="utf-8") as output_file:
        output_file.write("".join(sql_inserts))
    
    return sql_inserts

        
if __name__ == "__main__":
    download_pages()
    parse_pages()
    generate_sql_inserts()
    
    global_end = time.time()
    print(f"\nTotal time elapsed: {round(global_end - global_start, 3)}s")
