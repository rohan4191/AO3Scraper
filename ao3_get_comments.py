from bs4 import BeautifulSoup
import requests
from time import sleep
import argparse
import csv
from datetime import datetime
from unidecode import unidecode
import mysql.connector

# returns ID of the comment and saves it to database
def get_single_comment(db, cursor, ficid, comment, parentID):
    # Get comment id
    commentid = comment['id'].split('_')[1]

    # check if comment already in db, if so, pass
    sql = "SELECT COUNT(*) FROM fics.comments WHERE id = %s"
    val = (commentid, )
    cursor.execute(sql, val)
    if cursor.fetchone()[0] > 0:
        print("Duplicate work:", commentid)
        return
    
    print("Scraping comment ID:", commentid)

    # if no header, probably a deleted comment
    if comment.find('h4', class_='heading byline') == None:
        sql = "INSERT INTO comments (fic_id, id, parent_id) VALUES (%s, %s, %s)"
        val = (ficid, commentid, parentID)
        print("Deleted comment:", val)
        cursor.execute(sql, val)

        # save to db after each comment
        db.commit()

        return commentid
    
    # Find username
    if comment.find('h4', class_='heading byline').find('a'):
        username = comment.find('h4', class_='heading byline').find('a').contents[0]
    else:
        username = comment.find("h4", class_="heading byline").find("span").text

    # get chapter number
    # each comment has header "username on Chapter x" so get that text and split on spaces to isolate number
    # if single chap fic, no header, so default to 1
    header = comment.find("span", class_="parent")
    if header:
        chapternumber = int(header.text.split(" ")[-1])
    else:
        chapternumber = 1
        
    # Get datetime
    dateElem = comment.find('h4', class_='heading byline').find('span', class_="posted datetime").contents
    remove = ['\n', ' ']
    dateDict = {}

    for item in dateElem:
        if item not in remove:
            itemClass = item['class'][0]
            itemValue = item.contents[0]
            dateDict[itemClass] = itemValue

    dateObj = datetime.strptime(f'{dateDict["year"]}, {dateDict["month"]}, {dateDict["date"]}, {dateDict["time"]}', "%Y, %b, %d, %I:%M%p")



    # Get direct comment text
    text = comment.findAll("p")
    text = [str(p).replace("<br/>", "\n").replace("<p>", "").replace("</p>", "") for p in text]
    text = "\n".join(text)

    # print out comment data
    # if parent ID is 0 means that no parent comment, so set null
    if parentID == 0:
        parentID = None
    sql = "INSERT INTO comments VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (ficid, commentid, chapternumber, username, dateObj, parentID, text)
    print(val)
    cursor.execute(sql, val)

    # save to db after each comment
    db.commit()

    return commentid


# recursively loops over thread of comments
def get_comment_thread(db, cursor, ficid, thread, parentID):    
    # get individual comments
    comments = thread.findChildren("li", recursive=False)

    # track what id the parent comment should be
    newParentID = parentID
    for c, comment in enumerate(comments):
        # if only attr is class=commant, it's a collapsed thread we need to open
        if comment.attrs == {'class': ['comment']}:
            url = "http://archiveofourown.org" + comment.find("a")["href"]
            print("Expanding thread:", url)
            status = 429
            while 429 == status:
                req = requests.get(url)
                status = req.status_code
                if 429 == status:
                    print("Request answered with Status-Code 429")
                    print("Trying again in 1 minute...")
                    sleep(60)
            # for other errors, halt scraping
            if 400 <= status:
                print("Error:", status, ", halting scraping on fic", ficid)
                return
            src = req.text
            soup = BeautifulSoup(src, 'html.parser')
            thread = soup.find("ol", class_="thread")
            get_comment_thread(db, cursor, ficid, thread, newParentID)
            
        # if comments has attrs, it's a single comment
        elif comment.attrs != {}:
            # if we encounter a thread, that thread's parent will always be the most recent single comment
            # so maintain the ID of most recent single comment as parent ID
            newParentID = get_single_comment(db, cursor, ficid, comment, parentID)
            
        # if no attrs, it's a thread -- meaning it is a child of the previous comment
        else:
            thread = comment.findChild("ol")
            get_comment_thread(db, cursor, ficid, thread, newParentID)


def get_comment_page(db, cursor, ficid, pagenum):
    url = 'http://archiveofourown.org/works/'+str(ficid)+'?view_adult=true&amp;view_full_work=true&show_comments=true&page='\
    + str(pagenum)
    print("Scraping URL:", url)
    
    status = 429
    while 429 == status:
        req = requests.get(url)
        status = req.status_code
        if 429 == status:
            print("Request answered with Status-Code 429")
            print("Trying again in 1 minute...")
            sleep(60)
    # for other errors, write out to csv and pass
    if 400 <= status:
        print("Error:", status, ", halting scraping on page", pagenum)
        return
    
    src = req.text
    soup = BeautifulSoup(src, 'html.parser')
    
    thread = soup.find('ol', class_ = 'thread')
    if not thread:
        print(f"Fic {ficid} has no comments on page {pagenum}")
        return
    
    get_comment_thread(db, cursor, ficid, thread, 0)


def get_all_comments(db, cursor, ficid, restart_pagenum):
    url = 'http://archiveofourown.org/works/'+str(ficid)+'?view_adult=true&amp;view_full_work=true&show_comments=true'
    
    status = 429
    while 429 == status:
        req = requests.get(url)
        status = req.status_code
        if 429 == status:
            print("Request answered with Status-Code 429")
            print("Trying again in 1 minute...")
            sleep(60)
    # for other errors, write out to csv and pass
    if 400 <= status:
        print("Error:", status, ", halting scraping on fic", ficid)
        return
    
    src = req.text
    soup = BeautifulSoup(src, 'html.parser')
    
    # check to see if enough comments for multiple pages
    if (soup.find('ol', class_='pagination actions')):
        # get max page num
        numpages = int(soup.find('ol', class_='pagination actions').findChildren("li", recursive=False)[-2].text)
        # get comments for each page
        for i in range(numpages):
            get_comment_page(db, cursor, ficid, restart_pagenum + i)
            
    # if only one page of comments
    else:
        get_comment_page(db, cursor, ficid, 1)


def get_args(): 
    parser = argparse.ArgumentParser(description='Scrape the comments on a given csv of AO3 work IDs.')
    parser.add_argument(
        'ids', metavar='IDS', nargs='+',
        help='a single id, a space seperated list of ids, or a csv input filename')
    parser.add_argument(
        '--restart', default='', 
        help='work_id to start at from within a csv')
    parser.add_argument(
        '--page', default=0, 
        help='page number to restart from')
    args = parser.parse_args()
    fic_ids = args.ids
    is_csv = (len(fic_ids) == 1 and '.csv' in fic_ids[0]) 
    restart = str(args.restart)
    page = args.page
    return fic_ids, restart, is_csv, page

def main():
     # connect to database
    db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "password",
        database = "fics"
    )
    cursor = db.cursor()

    fic_ids, restart, is_csv, page = get_args()
    
    if not is_csv:
        print("Not csv")
        print("Page arg:", page)
        get_all_comments(db, cursor, fic_ids[0])
        return

    start = False
    if restart == '': start = True
    
    with open(fic_ids[0], "r+", newline="") as f_in:
        print("CSV")
        reader = csv.reader(f_in)
        for row in reader:
            if type(row[0]) != int:
                print("Row not of type int:", row)
                continue
            print("Page:", page)
            if not row: continue
            
            # ignore until we reach row to restart scrape from
            if not start:
                if row[0] != restart: continue
                start = True
            
            # get all comments for fic id
            get_all_comments(db, cursor, row[0], page)
            page = 1
    

main()