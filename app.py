from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from flask_cors import CORS, cross_origin
from requests_html import AsyncHTMLSession
import asyncio, pyppeteer, os, requests
import mysql.connector as conn
import base64
import pymongo
import pandas as pd

app = Flask(__name__)


# while running in local, use all your database credential
# the database will be build as you keep scrapping more data
# once there is enough data then we can extract data directly from database to run any analysis
# this code doesnt use any predefined API's
# however the code implies heavy RAM usage as it renders a JS webpage to do all scrapping in background.
# please tweak the scrolldown parameter to vary the results
# the database consists of two SQL table one NOSQL collection
# since it is a scrapping project, only scrapped results are displayed to user
# measures to avoid data redundancy have been implemented

host = 'appdbscrapper.curso3av0eit.us-east-1.rds.amazonaws.com'
user = 'admin'
'''
host = 'localhost'
user = 'root'
'''

# Establish mongodb connection
client = pymongo.MongoClient("mongodb+srv://achowdh:failsafe@agnik0.mmtem.mongodb.net/?retryWrites=true&w=majority")
db = client.test
# create database and collection
database = client['YoutubeScrapper']
collection = database["image64"]
# created connection to MYSQL host
mydb = conn.connect(host=host, user=user, passwd="Failsafe#1")
cursor = mydb.cursor()




# create database , if already exists it will ignore
cursor.execute("CREATE DATABASE IF NOT EXISTS dbYoutube")
query = "CREATE TABLE IF NOT EXISTS dbYoutube.links_table(video_id varchar(200) primary key not null,Channel_id varchar(200),Title varchar(250) ,Links varchar(250))"
cursor.execute(query)
mydb.commit()


# create table , if already exists it will ignore
query_1 = "CREATE TABLE IF NOT EXISTS dbYoutube.video_details_table(video_id varchar(200) primary key not null,vid_likes varchar(200),vid_views varchar(100),vid_comments varchar(100))"
cursor.execute(query_1)
mydb.commit()

# use table
cursor.execute("USE dbYoutube")


# this function is used to insert channel video data into table
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

# this function is used to insert scrapped video data into a table
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

        # here any existing video data will be updated in table
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


# async function to scrap each video details

async def scrap_video_content(url):

    new_loop = asyncio.new_event_loop()     # a new event loop is created
    asyncio.set_event_loop(new_loop)
    session = AsyncHTMLSession()            # session is being used as an instance of AsyncHTMLsession() class
    browser = await pyppeteer.launch({      # launch pyppeteer browser with below parameters, this will help the browser to run inside a flask routine
        'ignoreHTTPSErrors': True,          # otherwise it will throw errors.
        'headless': True,
        'handleSIGINT': False,
        'handleSIGTERM': False,
        'handleSIGHUP': False,
        'autoClose': False,
        'args':["--no-sandbox"]
    })
    session._browser = browser
    r = await session.get(url)              # the URL passed from HTML page is received here

    # the html page is being rendered with below parameters, to scrolldown for more details, increase the scrolldown parameters.
    # scrolldown 30 will extract about 200 comments from a video.

    await r.html.arender(sleep=1, keep_page=True, scrolldown=4,timeout=30)

    # extracting details with CSS class
    views = r.html.find('span.view-count.style-scope.ytd-video-view-count-renderer')
    likes = r.html.find('yt-formatted-string#text.style-scope.ytd-toggle-button-renderer.style-text')
    author = r.html.find('a#author-text')
    author_comment = r.html.find('yt-formatted-string#content-text')
    comment_count = r.html.find('yt-formatted-string.count-text.style-scope.ytd-comments-header-renderer')
    vid_title = r.html.find('h1.title.style-scope.ytd-video-primary-info-renderer')

    # extracting only video id's
    vid_id = url[url.find('=') + 1:]


    title = vid_title[0].text  # extract title of the video
    views = (views[0].text.split()[0])  # extract number of views
    comment_count = (comment_count[0].text.split()[0])  # extract number of comments
    likes = likes[0].text  # extract total likes

    author_list = [i.text for i in author]  # extracting authors of comments
    comments_list = [i.text for i in author_comment]  # extracting the comments

    data = []

    # appending the details in one list
    data.append(vid_id)
    data.append(title)
    data.append(views)
    data.append(likes)
    data.append(comment_count)

    # appending the author and comments in another list
    author_comments_list = list(zip(author_list, comments_list))

    r.close()
    browser.close()

    return data, author_comments_list       # return both the list

# async function to scrap each channel details
# similar pyppeteer browser used to scrap the rendered page

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

    # extracting all video titles and storing in a list
    video_titles = ([item.text for item in r.html.find('#video-title')])

    # extracting all video_links and storing in a list, this returns set of video links
    video_links = ([str(item.absolute_links) for item in r.html.find('#video-title')])

    # extracting the links from the above list by slicing operation
    link = [i[2:(len(i) - 2)] for i in video_links]

    # finding thumbnails
    thumb = r.html.find('div#items img.style-scope.yt-img-shadow[src]')

    # extracting thubmnails
    thumb_nail = []
    for item in thumb:
        d = item.attrs
        thumb_nail.append(d['src'])

    # storing all the details in a list
    data = list(zip(video_titles, link, thumb_nail))

    r.close()
    browser.close()

    return data # return the data

# a simple function to extract channel id from the URL
def youtubechannel_id(url):
    reverse_s = url[::-1]
    start = reverse_s.find('/')
    trunc_s = reverse_s[start + 1:]
    end_s = trunc_s.find('/')
    trunc_again = trunc_s[:end_s]
    final_s = trunc_again[::-1]
    return final_s

# this route is used to return end-user a excel file as an attachments of all the scrapped comments
@app.route('/scrapl/<vid_id>', methods=['GET', 'POST'])
@cross_origin()
def retrieve_data(vid_id):

    try:
        if not os.path.exists('./comments-data'): # create folder in root if folder not exists already
            os.makedirs('./comments-data')

        if os.path.exists(f"./comments-data/comments-{vid_id}.xlsx"):   # removes previosly file of same video so as to avoid repetition
            os.remove(f"./comments-data/comments-{vid_id}.xlsx")

        commentdata = collection.find({'vid_id': vid_id}) # retrieving data from mongodb server with video id
        listofcomments = []

        # appending the comments data
        for i in commentdata:
            listofcomments.append((i['comment_author'], i['comment']))

        # converting into pandas dataframe
        fi = pd.DataFrame(data=listofcomments, columns=['Author', 'Comment'])
        fi = fi.drop_duplicates()

        # converting dataframe to excel
        fi.to_excel(f"./comments-data/comments-{vid_id}.xlsx")
        # access the file from root to be returned to flask
        file_to_send = f"./comments-data/comments-{vid_id}.xlsx"

    except Exception as e:

        print(f"Unable to download file")

    return send_file(file_to_send, as_attachment=True) # return the excel file

# route to scrap video details from  each video link
@app.route('/scrapl', methods=['POST', 'GET'])
@cross_origin()
def try_page():
    if request.method == 'POST':

        try:

            # requesting data from HTML page ( here each video link is being passed )
            search_string = request.form['url']
            # print(search_string)

            # calling the scrap_video_content function as a co-routine
            sub_launch = asyncio.run(scrap_video_content(search_string))
            # print(sub_launch)

            # storing the returned data as a dictionary
            d = {
                "vid_id": sub_launch[0][0],
                "vid_title": sub_launch[0][1],
                "vid_view": sub_launch[0][2],
                "vid_likes": sub_launch[0][3],
                "vid_comments": sub_launch[0][4],
                "vid_auth_comment": sub_launch[1]

            }
            # passing the details to be entered into SQL database
            insert_data_details(d)

            # storing the comments in a dictionary to be passed to mongodb
            for i in sub_launch[1]:
                write_comment = {
                    "vid_id": sub_launch[0][0],
                    "comment_author": i[0],
                    "comment": i[1]
                }

                # checking for existing comments in database, if found data is not inserted, to avoid duplicate data
                if collection.find_one({'$and': [{'vid_id':sub_launch[0][0]}, {'comment_author': i[0]}, {'comment':i[1]}]}) == None:

                    collection.insert_one(write_comment)
                    print("comment inserted")
                else:
                    print("comment already exists , hence skipped database process")

            list_to_pass = []
            list_to_pass.append(d) # a list appended with all details to be passed to HTML page

        except Exception as e:

            print('The Exception message is: ', e)
    # the HTML template will display the comments and video details
    return render_template('scrap.html', list_to_pass=list_to_pass)


@app.route('/', methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")


@app.route('/review', methods=['POST', 'GET'])  # route to show the video details in a web UI
@cross_origin()
def test_run():
    if request.method == 'POST':
        try:

            # request data from webpage
            search_string = request.form['content']

            # call scrap_video_url fucntion as a co-routine
            rr = asyncio.run(scrap_video_url(search_string))

            list_content = []

            # function call to extract channel id
            channelid = youtubechannel_id(search_string)
            # print(f"scrapped url : {len(rr)}")

            # folder to store extracted thumbnails
            folder_path = './images'
            target_folder = os.path.join(folder_path, '_'.join(channelid.split(' ')))
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)


            count = 1 # counter variable
            for items in rr:

                # to check for shorts videos and perform slicing operation to extract video id's
                # shorts video id's differ from normal videos
                if items[1].find('=') == -1:
                    vid_id = items[1][items[1].find("shorts") + 7:]
                else:
                    vid_id = items[1][items[1].find('=') + 1:]

                # dictionary to store the extracted details
                d = {
                    "Sl_No": i,
                    "Vid_id": vid_id,
                    "Channel_id": channelid,
                    "Title": items[0],
                    "Link": items[1],
                    "Thumbnail": items[2],

                }
                count += 1

                # function call to store the thumbnail in root and mongodb
                save_thumbnail(target_folder, items[2], vid_id)

                # store data in SQL table
                insert_data(d)
                list_content.append(d)

                if count == 51: # counter variable condition check to show top 50 videos
                    break

            return render_template('results.html', list_content=list_content)
        except Exception as e:
            print('The Exception message is: ', e)
            return 'something is wrong'

    else:
        return render_template('index.html')

# function to save the thumbnails
def save_thumbnail(folder_path: str, url: str, vid_id):

    try:
        # converting thumbnail links into base64
        image_content = requests.get(url).content
        convert64 = base64.b64encode(image_content)

        # dictionary to be passed to mongodb of thumbnail data
        d = {
            "video_id": vid_id,
            "base64_thumb": convert64
        }

        # extract data from mongodb to avoid duplicate entry of thumbnails

        to_check = collection.find_one({'video_id': vid_id})
        print(to_check)

        # ignores if thumbnail already exists in database
        if to_check == None:
            collection.insert_one(d)
            print(vid_id, "  entered")

        elif to_check['video_id'] == vid_id:

            print("Thumbnail already exists")

    except Exception as e:
        print(f"ERROR - Could not save image {url} - {e}")

    try:

        # save image to root folder by converting the byte64 format to JPG
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
