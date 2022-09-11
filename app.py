from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from flask_cors import CORS, cross_origin
from requests_html import AsyncHTMLSession
import asyncio, pyppeteer, os, requests
import mysql.connector as conn
import base64
import pymongo
import pandas as pd

app = Flask(__name__)

host = 'appdbscrapper.curso3av0eit.us-east-1.rds.amazonaws.com'
user = 'admin'
'''
host = 'localhost'
user = 'root'
'''
client = pymongo.MongoClient("mongodb+srv://achowdh:failsafe@agnik0.mmtem.mongodb.net/?retryWrites=true&w=majority")
db = client.test
database = client['YoutubeScrapper']
collection = database["image64"]

mydb = conn.connect(host=host, user=user, passwd="Failsafeauto#1")
cursor = mydb.cursor()
cursor.execute("USE dbYoutube")

cursor.execute("CREATE DATABASE IF NOT EXISTS dbYoutube")
query = "CREATE TABLE IF NOT EXISTS dbYoutube.links_table(video_id varchar(200) primary key not null,Channel_id varchar(200),Title varchar(250) ,Links varchar(250))"
cursor.execute(query)
mydb.commit()

query_1 = "CREATE TABLE IF NOT EXISTS dbYoutube.video_details_table(video_id varchar(200) primary key not null,vid_likes varchar(200),vid_views varchar(100),vid_comments varchar(100))"
cursor.execute(query_1)
mydb.commit()


def insert_data(dict_values):
    try:
        # mydb = conn.connect(host="localhost", user="root", passwd="Failsafeauto#1")
        # cursor = mydb.cursor()

        query = "INSERT IGNORE INTO dbYoutube.links_table values(\"{}\",\"{}\",\"{}\",\"{}\")".format(
            dict_values['Vid_id'],
            dict_values['Channel_id'],
            dict_values['Title'],
            dict_values['Link'])

        cursor.execute(query)
        mydb.commit()


    except Exception as e:

        print(f"unable to insert data into database(links_table) :", e)


def insert_data_details(dict_values):
    try:
        # mydb = conn.connect(host="localhost", user="root", passwd="Failsafeauto#1")
        # cursor = mydb.cursor()

        query = "INSERT IGNORE INTO dbYoutube.video_details_table values(\"{}\",\"{}\",\"{}\",\"{}\")".format(
            dict_values['vid_id'],
            dict_values['vid_likes'],
            dict_values['vid_view'],
            dict_values['vid_comments'])

        cursor.execute(query)
        mydb.commit()

        query = "UPDATE dbYoutube.video_details_table SET vid_likes = \"{}\", vid_views = \"{}\", vid_comments = \"{}\" WHERE video_id = \"{}\"".format(
            dict_values['vid_likes'],
            dict_values['vid_view'],
            dict_values['vid_comments'],
            dict_values['vid_id']
        )
        cursor.execute(query)
        mydb.commit()
        # print("data inserted into video_details_table")


    except Exception as e:

        print(f"unable to insert data into database(video_details_table)", e)


async def scrap_video_content(url):
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    session = AsyncHTMLSession()
    browser = await pyppeteer.launch({
        'ignoreHTTPSErrors': True,
        'headless': True,
        'handleSIGINT': False,
        'handleSIGTERM': False,
        'handleSIGHUP': False,
        'autoClose': False,
        'args':["--no-sandbox"]
    })
    session._browser = browser
    r = await session.get(url)

    await r.html.arender(sleep=1, keep_page=True, scrolldown=4,timeout=30)

    views = r.html.find('span.view-count.style-scope.ytd-video-view-count-renderer')
    likes = r.html.find('yt-formatted-string#text.style-scope.ytd-toggle-button-renderer.style-text')
    author = r.html.find('a#author-text')
    author_comment = r.html.find('yt-formatted-string#content-text')
    comment_count = r.html.find('yt-formatted-string.count-text.style-scope.ytd-comments-header-renderer')
    vid_title = r.html.find('h1.title.style-scope.ytd-video-primary-info-renderer')

    vid_id = url[url.find('=') + 1:]

    title = vid_title[0].text
    views = (views[0].text.split()[0])  # might need attention
    comment_count = (comment_count[0].text.split()[0])  # might need attention
    likes = likes[0].text

    author_list = [i.text for i in author]
    comments_list = [i.text for i in author_comment]

    data = []

    data.append(vid_id)
    data.append(title)
    data.append(views)
    data.append(likes)
    data.append(comment_count)

    author_comments_list = list(zip(author_list, comments_list))

    r.close()

    return data, author_comments_list


async def scrap_video_url(u):
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    session = AsyncHTMLSession()
    browser = await pyppeteer.launch({
        'ignoreHTTPSErrors': True,
        'headless': True,
        'handleSIGINT': False,
        'handleSIGTERM': False,
        'handleSIGHUP': False,
        'autoClose': False,
        'args':["--no-sandbox"]
    })
    session._browser = browser
    r = await session.get(u)

    await r.html.arender(sleep=1,keep_page=True, scrolldown=4,timeout=30)

    video_titles = ([item.text for item in r.html.find('#video-title')])

    video_links = ([str(item.absolute_links) for item in r.html.find('#video-title')])

    link = [i[2:(len(i) - 2)] for i in video_links]

    thumb = r.html.find('div#items img.style-scope.yt-img-shadow[src]')

    thumb_nail = []
    for item in thumb:
        d = item.attrs
        thumb_nail.append(d['src'])

    data = list(zip(video_titles, link, thumb_nail))

    r.close()

    return data


def youtubechannel_id(url):
    reverse_s = url[::-1]
    start = reverse_s.find('/')
    trunc_s = reverse_s[start + 1:]
    end_s = trunc_s.find('/')
    trunc_again = trunc_s[:end_s]
    final_s = trunc_again[::-1]
    return final_s


@app.route('/scrapl/<vid_id>', methods=['GET', 'POST'])
@cross_origin()
def retrieve_data(vid_id):

    try:
        if not os.path.exists('./comments-data'):
            os.makedirs('./comments-data')

        if os.path.exists(f"./comments-data/comments-{vid_id}.xlsx"):
            os.remove(f"./comments-data/comments-{vid_id}.xlsx")

        commentdata = collection.find({'vid_id': vid_id})
        listofcomments = []

        for i in commentdata:
            listofcomments.append((i['comment_author'], i['comment']))

        fi = pd.DataFrame(data=listofcomments, columns=['Author', 'Comment'])
        fi = fi.drop_duplicates()

        fi.to_excel(f"./comments-data/comments-{vid_id}.xlsx")
        file_to_send = f"./comments-data/comments-{vid_id}.xlsx"

    except Exception as e:

        print(f"Unable to download file")

    return send_file(file_to_send, as_attachment=True)


@app.route('/scrapl', methods=['POST', 'GET'])
@cross_origin()
def try_page():
    if request.method == 'POST':

        try:

            search_string = request.form['url']
            # print(search_string)

            sub_launch = asyncio.run(scrap_video_content(search_string))
            # print(sub_launch)

            d = {
                "vid_id": sub_launch[0][0],
                "vid_title": sub_launch[0][1],
                "vid_view": sub_launch[0][2],
                "vid_likes": sub_launch[0][3],
                "vid_comments": sub_launch[0][4],
                "vid_auth_comment": sub_launch[1]

            }
            insert_data_details(d)

            for i in sub_launch[1]:
                write_comment = {
                    "vid_id": sub_launch[0][0],
                    "comment_author": i[0],
                    "comment": i[1]
                }


                if collection.find_one({'$and': [{'vid_id':sub_launch[0][0]}, {'comment_author': i[0]}, {'comment':i[1]}]}) == None:

                    collection.insert_one(write_comment)
                    print("comment inserted")
                else:
                    print("comment already exists , hence skipped database process")

            list_to_pass = []
            list_to_pass.append(d)

        except Exception as e:

            print('The Exception message is: ', e)

    return render_template('scrap.html', list_to_pass=list_to_pass)


@app.route('/', methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")


@app.route('/review', methods=['POST', 'GET'])  # route to show the review comments in a web UI
@cross_origin()
def test_run():
    if request.method == 'POST':
        try:

            search_string = request.form['content']

            rr = asyncio.run(scrap_video_url(search_string))

            list_content = []
            channelid = youtubechannel_id(search_string)
            # print(f"scrapped url : {len(rr)}")
            folder_path = './images'
            target_folder = os.path.join(folder_path, '_'.join(channelid.split(' ')))
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)
            i = 1
            for items in rr:

                if items[1].find('=') == -1:
                    vid_id = items[1][items[1].find("shorts") + 7:]
                else:
                    vid_id = items[1][items[1].find('=') + 1:]

                d = {
                    "Sl_No": i,
                    "Vid_id": vid_id,
                    "Channel_id": channelid,
                    "Title": items[0],
                    "Link": items[1],
                    "Thumbnail": items[2],

                }
                i += 1

                save_thumbnail(target_folder, items[2], vid_id)

                insert_data(d)
                list_content.append(d)

                if i == 51:
                    break

            return render_template('results.html', list_content=list_content)
        except Exception as e:
            print('The Exception message is: ', e)
            return 'something is wrong'

    else:
        return render_template('index.html')


def save_thumbnail(folder_path: str, url: str, vid_id):
    try:
        image_content = requests.get(url).content
        convert64 = base64.b64encode(image_content)

        d = {
            "video_id": vid_id,
            "base64_thumb": convert64
        }

        to_check = collection.find_one({'video_id': vid_id})
        print(to_check)

        if to_check == None:
            collection.insert_one(d)
            print(vid_id, "  entered")

        elif to_check['video_id'] == vid_id:

            print("Thumbnail already exists")

    except Exception as e:
        print(f"ERROR - Could not save image {url} - {e}")

    try:

        file_path = f"./{folder_path}/{vid_id}.jpg"

        if not os.path.exists(file_path):
            f = open(os.path.join(folder_path, str(vid_id) + ".jpg"), 'wb')
            f.write((base64.decodebytes(convert64)))
            f.close()
            print(f"SUCCESS - saved {url} - as {folder_path}")
        # print("Image already exists")

    except Exception as e:
        print(f"ERROR - Could not save {url} - {e}")


if __name__ == "__main__":
    # app.run(host='127.0.0.1', port=8001, debug=True)
    app.run(debug=True)
