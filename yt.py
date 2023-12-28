from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connecion
def Api_connect():
    Api_Id="AIzaSyDYFHHrXgnchgeEZ_Iv4KXqp7UgQz85ngQ"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channels info
def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
        )
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_Name = i["snippet"]["title"],
            Channel_Id = i['id'],
            Subscribers= i['statistics']['subscriberCount'],
            Views = i['statistics']['viewCount'],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description = i["snippet"]['description'],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


#get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part="snippet",
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# get video info
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item  in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_id=item['snippet']['channelId'],
                    VideoId=item['id'],
                    Title=item['snippet']['title'],
                    Thumbmail=item['snippet']['thumbnails']['default']['url'],
                    Tag=item['snippet'].get('tags'),
                    Description=item['snippet'].get('description'),
                    Publish_At=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Fav_Count=item['statistics'].get('favoriteCount'),
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)
    return(video_data)


#get Comment info
def get_Comment_info(video_ids):
     Comment_data=[]
     try:
          for video_id in video_ids:
               request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50)
               response = request.execute()

               for i in response['items']:
                    data=dict(Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                              Comment_ID=i['snippet']['topLevelComment']['id'],
                              Comment=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                              Author_Name=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                              Author_Channel_Id=i['snippet']['topLevelComment']['snippet']['authorChannelId'],
                              Published_At=i['snippet']['topLevelComment']['snippet']['publishedAt'],
                              Like_Count=i['snippet']['topLevelComment']['snippet']['likeCount'])
                    Comment_data.append(data)
     except:
          pass
     return Comment_data

#get Playlist Details
def get_playlist_info(channel_id):
    next_page_token=None
    Playlist_data=[]
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for i in response['items']:
            data=dict(
                Playlist_Id=i['id'],
                Playlist_Title=i['snippet']['title'],
                Video_Count=i['contentDetails']['itemCount'],
                Published_At=i['snippet']['publishedAt'])
            Playlist_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break      
    return Playlist_data


client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client["Youtube_Data"]


def Channel_Details(channel_id):
    Ch_details=get_channel_info(channel_id)
    Pl_details=get_playlist_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_Comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":Ch_details,"playlist_information":Pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "Upload Completed Successfully"


#table 
def channels_table():
    mydb=psycopg2.connect(host="localhost",
                            port="5432",
                            user="postgres",
                            password="Andrew1425$",
                            database="Youtube_Data")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channel'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channel(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table Already created")


    ch_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channel(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id )
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
            
        except:
            print("Channels values are already inserted")


def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                            port="5432",
                            user="postgres",
                            password="Andrew1425$",
                            database="Youtube_Data")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Playlist_Title varchar(100),
                                                        Published_At timestamp,
                                                        Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Playlist_Title,
                                            Video_Count,
                                            Published_At)

                                            values(%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Playlist_Title'],
                row['Video_Count'],
                row['Published_At']
                )
        cursor.execute(insert_query,values)
        mydb.commit()


def video_table():
    mydb=psycopg2.connect(host="localhost",
                            port=5432,
                            user="postgres",
                            password="Andrew1425$",
                            database="Youtube_Data")
    cursor=mydb.cursor()

    drop_query='''drop table if exists video'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists video(Channel_Name varchar(100),
                                                    Channel_id varchar(100),
                                                    VideoId varchar(30),
                                                    Title varchar(150),
                                                    Thumbmail varchar(200),
                                                    Tag text,
                                                    Description text,
                                                    Publish_At timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Fav_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
        insert_query='''insert into video(Channel_Name,
                                            Channel_id,
                                            VideoId,
                                            Title,
                                            Thumbmail,
                                            Tag,
                                            Description,
                                            Publish_At,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Fav_Count,
                                            Definition,
                                            Caption_Status)

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_id'],
                row['VideoId'],
                row['Title'],
                row['Thumbmail'],
                row['Tag'],
                row['Description'],
                row['Publish_At'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Fav_Count'],
                row['Definition'],
                row['Caption_Status'])

        cursor.execute(insert_query,values)
        mydb.commit()

def comment_table():
    mydb=psycopg2.connect(host="localhost",
                            port="5432",
                            user="postgres",
                            password="Andrew1425$",
                            database="Youtube_Data")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_ID varchar(100) primary key,
                                Video_Id varchar(50),
                                Comment text,
                                Author_Name varchar(150),
                                Author_Channel_Id varchar(100),
                                Published_At timestamp,
                                Like_Count bigint)'''

    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_ID,
                                                    Video_Id,
                                                    Comment,
                                                    Author_Name,
                                                    Author_Channel_Id,
                                                    Published_At,
                                                    Like_Count
                                                    )   
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Comment_ID'],
                    row['Video_Id'],
                    row['Comment'],
                    row['Author_Name'],
                    row['Author_Channel_Id'],
                    row['Published_At'],
                    row['Like_Count']
                    )

            cursor.execute(insert_query,values)
            mydb.commit()

def tables():
    channels_table()
    playlist_table()
    video_table()
    comment_table()

    return "Tables Created Successfully"

def Show_channel_table():
    ch_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_video_table():
    vi_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comment_table():
    com_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

#streamlit part

st.image('https://images.app.goo.gl/nRcpr8YTHYB99p3m6', caption='Sunrise by the mountains')
with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVERSTING AND WAREHOUSING]")
    st.header(":red[Skills Take Away]")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    st.header(":red[By Infant Andrew R]")

channel_id=st.text_input("Enter the channel id")

if st.button("Collect and Store Data To MongoDB:"):
    ch_ids=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id was already exists")

    else:
        insert=Channel_Details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    Show_channel_table()
elif show_table=="PLAYLISTS":
    show_playlist_table()
elif show_table=="VIDEOS":
    show_video_table()
elif show_table=="COMMENTS":
    show_comment_table()



#sql connection

mydb=psycopg2.connect(host="localhost",
                        port=5432,
                        user="postgres",
                        password="Andrew1425$",
                        database="Youtube_Data")
cursor=mydb.cursor()

question=st.selectbox("Select Your Question",("1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from video'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df1)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_videos from channel'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)    

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname,title as videotitle from video
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3) 

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments as no_comments,title as videotitle from video
                where comments is not null '''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df4)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title as videotitle,channel_name as channelname,likes as like_count from video
                where likes is not null order by likes desc '''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","like_count"])
    st.write(df5)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select title as videotitle,channel_name as channelname,likes as like_count from video
            where likes is not null '''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["videotitle","channelname","like_count"])
    st.write(df6)

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name as channelname,views as totalviews from channel
                '''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","like_count"])
    st.write(df7)

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as video_title,publish_at as videorelease,channel_name as channelname from video
                where extract(year from publish_at)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video_title","videorelease","channelname"])
    st.write(df8)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from video
                group by channel_name '''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","average duration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["average duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df=pd.DataFrame(T9) 
    st.write(df) 

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from video
                where comments is not null order by comments desc '''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)



st.header(":red[THANK YOU]")