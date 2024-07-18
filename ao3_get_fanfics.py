######
#
# This script takes in (a list or csv of) fic IDs and
# writes a csv containing the fic itself, as well as the 
# metadata.
#
# Usage - python ao3_get_fanfics.py ID
#
# ID is a required argument. It is either a single number, 
# multiple numbers seperated by spaces, or a csv filename where
# the IDs are the first column.
# (It is suggested you run ao3_work_ids.py first to get this csv.)
#
# --restart is an optional string which when used in combination with a csv input will start
# the scraping from the given work_id, skipping all previous rows in the csv
#
# Author: Jingyi Li soundtracknoon [at] gmail
# I wrote this in Python 2.7. 9/23/16
# Updated 2/13/18 (also Python3 compatible)
#######
import requests
from bs4 import BeautifulSoup
import argparse
import csv
from unidecode import unidecode
import mysql.connector

# seconds to wait between page requests
delay = 5

    
def get_stats(meta):
    '''
    returns a list of  
    language, published, status, date status, words, chapters, comments, kudos, bookmarks, hits
    '''
    categories = ['language', 'published', 'status', 'words', 'chapters', 'comments', 'kudos', 'bookmarks', 'hits'] 

    stats = list(map(lambda category: meta.find("dd", class_=category), categories))

    if not stats[2]:
        stats[2] = stats[1] #no explicit completed field -- one shot
    try:		
        stats = [unidecode(stat.text) for stat in stats]
    except AttributeError as e: #for some reason, AO3 sometimes miss stat tags (like hits)
        new_stats = []
        for stat in stats:
            if stat: new_stats.append(unidecode(stat.text))
            else: new_stats.append('null')
        stats = new_stats

    stats[0] = stats[0].rstrip().lstrip() #language has weird whitespace characters
    #add a custom completed/updated field
    status  = meta.find("dt", class_="status")
    if not status: status = 'Completed' 
    else: status = status.text.strip(':')
    stats.insert(2, status)

    return stats      

def get_tag_info(category, meta):
    '''
    given a category and a 'work meta group, returns a list of tags (eg, 'rating' -> 'explicit')
    '''
    try:
        tag_list = meta.find("dd", class_=str(category) + ' tags').find_all(class_="tag")
    except AttributeError as e:
        return []
    return [unidecode(result.text) for result in tag_list] 

def get_tags(meta):
    '''
    returns a list of lists, of
    rating, category, fandom, pairing, characters, additional_tags
    '''
    tags = ['rating', 'category', 'fandom', 'relationship', 'character', 'freeform']
    return list(map(lambda tag: get_tag_info(tag, meta), tags))

# get kudos
def get_kudos(meta):
    if (meta):
        users = []
        ## hunt for kudos' contents
        kudos = meta.contents

        # extract user names
        for kudo in kudos:
            if kudo.name == 'a':
                if 'more users' not in kudo.contents[0] and '(collapse)' not in kudo.contents[0]:
                    users.append(kudo.contents[0])
        
        return users
    return []

# get author(s)
def get_authors(meta):
    tags = meta.contents
    authors = []

    for tag in tags:
        if tag.name == 'a':
            authors.append(tag.contents[0])

    return authors

def access_denied(soup):
    if (soup.find(class_="flash error")):
        return True
    if (not soup.find(class_="work meta group")):
        return True
    return False

def write_fic_to_db(fic_id):
    print('Scraping ', fic_id)
    url = 'http://archiveofourown.org/works/'+str(fic_id)+'?view_adult=true&amp;view_full_work=true'
    print(url)
    req = requests.get(url)

    src = req.text
    soup = BeautifulSoup(src, 'html.parser')
    if (access_denied(soup)):
        print('Access Denied')
    else:
        meta = soup.find("dl", class_="work meta group")
        author = get_authors(soup.find("h3", class_="byline heading"))
        tags = get_tags(meta)
        stats = get_stats(meta)
        title = unidecode(soup.find("h2", class_="title heading").string).strip()
        
    #get the fic itself
    content = soup.find_all("div", class_ = "chapter")
    if not content:
        content = [soup.find("div", id = "chapters")] # if single chapter work, do this
            
    # connect to database
    db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "password",
        database = "fics"
    )
    print(db)
    cursor = db.cursor()

    # write metadata to table
    #     tags = ['rating', 'category', 'fandom', 'relationship', 'character', 'freeform']
    #     categories = ['language', 'published', 'status', 'date status', 'words', 'chapters', 'comments', 'kudos', 'bookmarks', 'hits'] 
    sql = "INSERT INTO works VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (fic_id, title, ", ".join(author), ", ".join(tags[0]), ", ".join(tags[1]), ", ".join(tags[2]), ", ".join(tags[3]), ", ".join(tags[4]), ", ".join(tags[5]), \
           stats[0], stats[1], stats[2], stats[3], int(stats[4].replace(',', '')), stats[5], int(stats[6].replace(',', '')), int(stats[7].replace(',', '')), int(stats[8].replace(',', '')), int(stats[9].replace(',', '')))
    cursor.execute(sql, val)
 
 
    # write fic chaps to table
    # [work id, chapter number, chapter text]
    sql = "INSERT INTO chaps VALUES (%s, %s, %s, %s, %s, %s, %s)"
    chapters = soup.select("div[class=chapter]")
    # case for single-chapter work
    if not chapters:
        title = soup.select_one(".title.heading")
        if title: title = title.text
        
        summary = soup.select_one(".summary.module")
        if summary: summary = summary.text
        
        notes = soup.select_one(".notes.module")
        if notes: notes = notes.text
        
        endnotes = soup.select_one(".end.notes.module")
        if endnotes: endnotes = endnotes.text
        
        chapter = soup.select_one("div[id=chapters]")
        body = chapter.select_one(".userstuff")
        lines = body.select("p")
        text = "\n".join([unidecode(line.text) for line in lines])
        
        val = (fic_id, 1, title, summary, notes, endnotes, text)
        cursor.execute(sql, val)
    
    # multi-chapter case
    else:
        for i, chapter in enumerate(chapters):
            title = chapter.select_one(".title")
            if title: title = title.text
            
            summary = chapter.select_one("div[id=summary]")
            if summary: summary = summary.text
            
            notes = chapter.select_one("div[id=notes]")
            if notes: notes = notes.text
            
            endnotes = chapter.select_one(".end.notes.module")
            if endnotes: endnotes = endnotes.text
        
            body = chapter.select_one(".userstuff.module")
            lines = body.select("p")
            text = "\n".join([unidecode(line.text) for line in lines])
            
            val = (fic_id, i + 1, title, summary, notes, endnotes, text)
            cursor.execute(sql, val)
            
 
    # write comments to table
    # will have to scrape by chapter instead of by entire work...
    # actually each comment specifies which chapter it was on....

    db.commit()
    print('Done.')

def get_args(): 
    parser = argparse.ArgumentParser(description='Scrape and save some fanfic, given their AO3 IDs.')
    parser.add_argument(
        'ids', metavar='IDS', nargs='+',
        help='a single id, a space seperated list of ids, or a csv input filename')
    parser.add_argument(
        '--restart', default='', 
        help='work_id to start at from within a csv')
    args = parser.parse_args()
    fic_ids = args.ids
    is_csv = (len(fic_ids) == 1 and '.csv' in fic_ids[0]) 
    restart = str(args.restart)
    return fic_ids, restart, is_csv

def process_id(fic_id, restart, found):
    if found:
        return True
    if fic_id == restart:
        return True
    else:
        return False

def main():
    fic_ids, restart, is_csv = get_args()
    
    with open(fic_ids[0], "r+", newline="") as f_in:
        reader = csv.reader(f_in)
        for row in reader:
            if not row: continue        
            write_fic_to_db(row[0])

main()
