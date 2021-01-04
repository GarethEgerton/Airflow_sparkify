class SqlQueries:

    # DROP TABLES
    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    user_table_drop = "DROP TABLE IF EXISTS users"
    song_table_drop = "DROP TABLE IF EXISTS song"
    artist_table_drop = "DROP TABLE IF EXISTS artists"
    time_table_drop = "DROP TABLE IF EXISTS time"

    drop_tables = '''
    DROP TABLE IF EXISTS staging_events;
    DROP TABLE IF EXISTS staging_songs;
    DROP TABLE IF EXISTS songplays;
    DROP TABLE IF EXISTS users;
    DROP TABLE IF EXISTS songs;
    DROP TABLE IF EXISTS artists;
    DROP TABLE IF EXISTS time;
    '''

    # CREATE TABLES
    create_tables ='''
    CREATE TABLE public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);

CREATE TABLE public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);

CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);

CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);

CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);
'''


    staging_events_table_create = ("""                 
                                    CREATE TABLE IF NOT EXISTS staging_events (
                                        artist varchar NOT NULL,
                                        auth varchar NOT NULL,
                                        firstName varchar, 
                                        gender varchar(1), 
                                        itemInSession int NOT NULL,
                                        lastName varchar,
                                        length float NOT NULL,
                                        level varchar NOT NULL,
                                        location varchar,
                                        method varchar(8),
                                        page varchar NOT NULL,
                                        registration int8,
                                        sessionId int NOT NULL,
                                        song varchar NOT NULL,
                                        status int,
                                        ts bigint NOT NULL,
                                        useragent varchar,
                                        userId int NOT NULL
                                    );
    """)

    staging_songs_table_create = ("""
                                    CREATE TABLE IF NOT EXISTS staging_songs (
                                        artist_id varchar NOT NULL,
                                        artist_latitude float,
                                        artist_location varchar,
                                        artist_longitude float,
                                        artist_name varchar NOT NULL,
                                        duration float NOT NULL,
                                        num_songs int,
                                        song_id varchar NOT NULL,
                                        title varchar NOT NULL,
                                        year int NOT NULL
                                        );
    """)

    songplay_table_create = ("""
                                CREATE TABLE IF NOT EXISTS songplay (
                                    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                                    start_time timestamp NOT NULL, 
                                    user_id int NOT NULL, 
                                    level varchar, 
                                    song_id varchar NOT NULL, 
                                    artist_id varchar NOT NULL, 
                                    session_id int NOT NULL, 
                                    location varchar, 
                                    user_agent varchar
                                    );
                            """)

    user_table_create = ("""
                            CREATE TABLE IF NOT EXISTS users (
                                user_id INT PRIMARY KEY, 
                                first_name varchar, 
                                last_name varchar, 
                                gender varchar(1), 
                                level varchar
                            );
    """)

    song_table_create = ("""
                            CREATE TABLE IF NOT EXISTS song (
                                song_id varchar PRIMARY KEY, 
                                title varchar NOT NULL, 
                                artist_id varchar NOT NULL, 
                                year int NOT NULL, 
                                duration float NOT NULL
                            );
    """)

    artist_table_create = ("""
                                CREATE TABLE IF NOT EXISTS artist (
                                    artist_id varchar PRIMARY KEY, 
                                    name varchar NOT NULL, 
                                    location varchar, 
                                    latitude float, 
                                    longitude float
                                );
    """)

    time_table_create = ("""
                            CREATE TABLE IF NOT EXISTS time (
                                start_time timestamp PRIMARY KEY, 
                                hour int NOT NULL, 
                                day int NOT NULL, 
                                week int NOT NULL, 
                                month int NOT NULL, 
                                year int NOT NULL, 
                                weekday in NOT NULL
                            );
    """)


    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)