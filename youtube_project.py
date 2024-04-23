# API Key

"AIzaSyA0dGX5qjjup-k_4whqcRh0wQzWeUzeyyU"


# Code

from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


# Api Key connection

def Api_connect():
    Api_Id="AIzaSyA0dGX5qjjup-k_4whqcRh0wQzWeUzeyyU"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()
    


# To get channel information:

def get_channel_details(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

# To get video ids:

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# To get video details:

def get_video_information(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for a in response["items"]:
            data=dict(Channel_Name=a['snippet']['channelTitle'],
                    Channel_Id=a['snippet']['channelId'],
                    Video_Id=a['id'],
                    Title=a['snippet']['title'],
                    Tags=a['snippet'].get('tags'),
                    Thumbnail=a['snippet']['thumbnails']['default']['url'],
                    Description=a['snippet'].get('description'),
                    Published_Date=a['snippet']['publishedAt'],
                    Duration=a['contentDetails']['duration'],
                    Views=a['statistics'].get('viewCount'),
                    Likes=a['statistics'].get('likeCount'),
                    Comments=a['statistics'].get('commentCount'),
                    Favorite_Count=a['statistics']['favoriteCount'],
                    Definition=a['contentDetails']['definition'],
                    Caption_Status=a['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data


# To get playlist details:

def get_playlist_details(channel_id):
        next_page_token=None
        All_playlistdata=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for i in response['items']:
                        data=dict(Playlist_Id=i['id'],
                                Title=i['snippet']['title'],
                                Channel_Id=i['snippet']['channelId'],
                                Channel_Name=i['snippet']['channelTitle'],
                                PublishedAt=i['snippet']['publishedAt'],
                                Video_Count=i['contentDetails']['itemCount'])
                        All_playlistdata.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_playlistdata


#To get comment details:

def get_comment_information(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for s in response['items']:
                data=dict(Comment_Id=s['snippet']['topLevelComment']['id'],
                        Video_Id=s['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=s['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=s['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=s['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

# Upload to mongoDB

client=pymongo.MongoClient("mongodb+srv://vmnandhini90:nandhini25@cluster0.gkghx5f.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["youtube_data"]

def channel_details(channel_id):

    ch_details=get_channel_details(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_information(vi_ids)
    com_details=get_comment_information(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                        "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"

# Table creation:

# Fetch channels information in postgre sql:

def channels_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="vmnandhini",
                            database="youtube_data",
                            port="5432")
    mycursor=mydb.cursor()
    

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        mycursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")
    
    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["channel_information"])

    df_single_channel= pd.DataFrame(single_channel_details)

    for index,row in df_single_channel.iterrows():
            insert_query='''insert into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id']
                    )

    try:
        mycursor.execute(insert_query,values)
        mydb.commit()

    except:
        news= f"Your Provided Channel {channel_name_s} is Already exists"        
        return news
    
# Fetch playlists information in postgre sql:

def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="vmnandhini",
                            database="youtube_data",
                            port="5432")
    mycursor=mydb.cursor()


    create_query='''create table if not exists playlists(Playlist_Id varchar(300) primary key,
                                                        Title varchar(300),
                                                        Channel_Id varchar(300),
                                                        Channel_Name varchar(300),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''

    mycursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["playlist_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])


    for index,row in df_single_channel.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count
                                                )
                                            
                                                values(%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count']
                )
    mycursor.execute(insert_query,values)
    mydb.commit()   

# Fetch videos information in postgre sql:

def videos_table(channel_name_s):

    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="vmnandhini",
                            database="youtube_data",
                            port="5432")
    mycursor=mydb.cursor()


    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50)
                                                        )'''
    mycursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["video_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])       


    for index,row in df_single_channel.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status
                                                    )
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status']
                        )

        
    mycursor.execute(insert_query,values)
    mydb.commit()      


# Fetch comment information in postgresql:

def comments_table(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="vmnandhini",
                                database="youtube_data",
                                port="5432")
        mycursor=mydb.cursor()


        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                                Video_Id varchar(50),
                                                                Comment_Text text,
                                                                Comment_Author varchar(150),
                                                                Comment_Published timestamp
                                                                )'''

        mycursor.execute(create_query)
        mydb.commit()
        
        single_channel_details= []
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
                single_channel_details.append(ch_data["comment_information"])

        df_single_channel= pd.DataFrame(single_channel_details[0])


        for index,row in df_single_channel.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published
                                                        )
                                                        
                                                        values(%s,%s,%s,%s,%s)'''
                
                
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published']
                        )

                
        mycursor.execute(insert_query,values)
        mydb.commit()


def tables(single_channel):

    news=channels_table(single_channel)
    if news:
        st.write(news)
    else:
        playlist_table(single_channel)
        videos_table(single_channel)
        comments_table(single_channel)

        return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def show_playlist_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2


def show_comments_table():
    com_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

#streamlit part

with st.sidebar:
    st.title(":green[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
            insert=channel_details(channel_id)
            st.success(insert)

all_channels= []
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
unique_channel= st.selectbox("Select the Channel",all_channels)

if st.button("Migrate to Sql"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

#SQL Connection

mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="vmnandhini",
                        database="youtube_data",
                        port="5432")
mycursor=mydb.cursor()

question=st.selectbox("Select your question",("1.All the videos and the channel name",
                                                "2. channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. comments in each videos",
                                                "5. Videos with higest likes",
                                                "6. likes of all videos",
                                                "7. views of each channel",
                                                "8. videos published in the year of 2022",
                                                "9. average duration of all videos in each channel",
                                                "10. videos with highest number of comments"))

if   question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)