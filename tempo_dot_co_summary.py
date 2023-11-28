import os
import pandas as pd
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from bs4 import BeautifulSoup


KEYWORDS_TO_REMOVE = ['Baca Juga', 'Pilihan Editor']

def extract_current_date():
    '''extract current date to scrap news'''
    # get raw date
    date_raw = pendulum.today()

    # extract year, month, day
    year = str(date_raw.year)
    month = str(date_raw.month)
    if len(month) == 1:
        month = '0' + month

    day = str(date_raw.day)    
    if len(day) == 1:
        day = '0' + day

    return year + '-' + month + '-' + day

def clean_detail_konten(konten_raws):
    '''parse detail konten html to str'''
    # Create a content_list
    content_list = []
    for konten_raw in konten_raws.findAll('p'):
        konten_raw_text = konten_raw.text
        konten_clean = konten_raw_text.rstrip().lstrip()
        content_list.append(konten_clean)

    # First, find index
    index_to_remove = []
    for i, konten in enumerate(content_list):
        for keywords in KEYWORDS_TO_REMOVE:
            if keywords in konten:
                index_to_remove.append(i)

                if keywords == 'Baca Juga':
                    index_to_remove.append(i+1)
                continue

    # Then remove the index
    clean_content_list = [konten for i, konten in enumerate(content_list) if i not in index_to_remove]
    
    # Finally join
    clean_konten = ' '.join(clean_content_list)

    return clean_konten

def extract_news_details(news_link):
    '''extract the news details'''
    # Request news
    news_raw = requests.get(news_link)
    news_soup_raw = BeautifulSoup(news_raw.text)

    # Extract info
    news_data = {}

    news_data['title'] = news_soup_raw.find('h1').text
    try:
        news_data['reporter'] = news_soup_raw.find('span', {'itemprop': 'author'}).text
    except:
        news_data['reporter'] = None 
    try:
        news_data['editor'] = news_soup_raw.find('span', {'itemprop': 'editor'}).text
    except:
        news_data['editor'] = None
    news_data['date_published'] = news_soup_raw.find('p', {'class': 'date margin-bottom-sm'}).text
    konten_raws = news_soup_raw.find('div', {'class': 'detail-konten'})
    news_data['konten'] = clean_detail_konten(konten_raws)
    tag_raws = news_soup_raw.find('div', {'class': 'box-tag-detail'})
    news_data['tag'] = ', '.join([tag.text for tag in tag_raws.findAll('a')])

    return news_data


@dag(
    dag_id = 'tempo_dot_co_summary',
    schedule_interval = '@hourly',
    start_date = pendulum.datetime(2023,9,17),
    catchup = False
)

def tempo_dot_co_summary():
    # First task
    @task()
    def get_current_news_brief_update():
        '''Get the news links'''
        # Get the current date
        current_date = extract_current_date()

        # Get current news update 
        news_raw = requests.get(f'https://www.tempo.co/indeks/{current_date}/')

        # Convert to soup for html parsing
        news_soup_raw = BeautifulSoup(news_raw.text)
        news_tag_all = news_soup_raw.findAll('div', {'class': 'card-box ft240 margin-bottom-sm'})

        # Find all link
        news_updates = []
        for news_tag in news_tag_all:
            # Create dict
            news_dict = {}

            # Parse the link
            for link_raw in news_tag.find('h2', 'title'):
                # Get the link
                news_dict['link'] = link_raw.get('href')
            
            # Parse the title
            for title_raw in news_tag.find('h2'):
                news_dict['title'] = str(title_raw).strip('</a>').split('>')[-1].rstrip()

            # Parse the headline descriptions
            for headline_raw in news_tag.find('p'):
                # Get the headline
                news_dict['headline'] = str(headline_raw).rstrip()

            # Append to news_links
            news_updates.append(news_dict)

        print('=================================')
        print(f'Found {len(news_updates)} news')
        print('=================================')
        print('')

        return news_updates

    # Second task 
    @task()
    def load_current_news_brief_update(news_briefs):
        '''Load the updated news'''
        # Get the hook
        hook = SqliteHook(sqlite_conn_id = 'tempo_news')
        
        # Get the currently stored
        stored = hook.get_pandas_df("SELECT * FROM news_brief;")

        # Start update
        new_news_briefs = []
        for news_brief in news_briefs:
            if news_brief['link'] not in stored['link'].values:
                new_news_briefs.append([news_brief['link'],
                                        news_brief['title'],
                                        news_brief['headline']])
                
        # Insert to the database
        hook.insert_rows(table = 'news_brief',
                         rows = new_news_briefs,
                         target_fields = ['link', 'title', 'headline'])
        
        print('=========================================')
        print(f'Write {len(new_news_briefs)} news brief')
        print('=========================================')
        print('')
        
        return None
        
    # Final Task
    @task()
    def load_current_news_detail_update(news_briefs):
        '''Load the updated news'''
        # Get the hook
        hook = SqliteHook(sqlite_conn_id = 'tempo_news')
        
        # Get the currently stored
        stored = hook.get_pandas_df("SELECT * FROM news_detail;")

        # Start update
        new_news_details = []
        for news_brief in news_briefs:
            if news_brief['link'] not in stored['link'].values:
                # print
                print('Extracted: ', news_brief['link'])

                # Get the news details
                news_details = extract_news_details(news_brief['link'])

                # Extract
                new_news_details.append([news_brief['link'],
                                         news_details['title'],
                                         news_details['reporter'],
                                         news_details['editor'],
                                         news_details['date_published'],
                                         news_details['konten'],
                                         news_details['tag']])
                
        # Insert to the database
        hook.insert_rows(table = 'news_detail',
                         rows = new_news_details,
                         target_fields = ['link', 'title', 'reporter', 'editor',
                                          'date_published', 'konten', 'tag'])
        
        print('============================================')
        print(f'Write {len(new_news_details)} news detail')
        print('============================================')
        print('')
        
        return None
        

    # 1. Get the news links updates
    news_briefs = get_current_news_brief_update()

    # 2. Then create the databases for news_brief
    create_links_brief_db = SqliteOperator(
        task_id = "create_table_brief",
        sql = r"""
        CREATE TABLE IF NOT EXISTS news_brief (
            link TEXT PRIMARY KEY,
            title TEXT,
            headline TEXT
        )
        """,
        sqlite_conn_id = 'tempo_news'
    )
    create_links_brief_db.set_downstream(news_briefs)

    # 3. Load the news briefs to the database
    loaded_brief = load_current_news_brief_update(news_briefs)
    loaded_detail = load_current_news_detail_update(news_briefs)

    # 4. Then create the databases for news_detail
    create_links_detail_db = SqliteOperator(
        task_id = "create_table_detail",
        sql = r"""
        CREATE TABLE IF NOT EXISTS news_detail (
            link TEXT PRIMARY KEY,
            title TEXT,
            reporter TEXT,
            editor TEXT,
            date_published TEXT,
            konten TEXT,
            tag TEXT
        )
        """,
        sqlite_conn_id = 'tempo_news'
    )
    create_links_detail_db.set_downstream(loaded_detail)


summary = tempo_dot_co_summary()
