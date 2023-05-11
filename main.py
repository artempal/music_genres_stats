import sqlite3
from bs4 import BeautifulSoup
import requests
from time import sleep

from collections import Counter
import sys

sys.path.insert(1, 'libpytunes')

from libpytunes import Library

# START SETTINGS BLOCK

DB_NAME = "music.sqlite"

IMPORT_FILENAME = "wave.xml"
IMPORT_FILE_TYPE = "xml"  # or "txt"
MERGE_WITH_ITUNES_GENRE = False  # or True
CLEAR_MUSIC_LIST_BEFORE_START = True  # or False

# END SETTINGS BLOCK


con = sqlite3.connect(DB_NAME)
cur = con.cursor()


def create_db():
    cur.execute("CREATE TABLE IF NOT EXISTS music_list(artists,name,genres)")
    cur.execute("CREATE TABLE IF NOT EXISTS artists_genres(artist,genres)")


def check_track_in_base(artists, name):
    params = (artists, name)
    res = cur.execute("SELECT name from music_list where artists = ? and name = ?", params)
    return res.fetchone()


def insert_track(artists, name, genres=None):
    if not check_track_in_base(artists, name):
        data = [(artists, name, genres)]
        cur.executemany("INSERT INTO music_list (artists, name,genres) VALUES (?,?,?)", data)
        con.commit()


def get_genres_api(artist):
    genres = set()
    sleep(1)
    response = requests.get(f"https://everynoise.com/lookup.cgi?who={artist}")
    soup = BeautifulSoup(response.text, 'lxml')
    soup.findAll('a')
    links = soup.find_all('a')
    links = links[:-2]  # clear list
    for link in links:
        genres.add(link.text)
    return ",".join(genres)


def insert_artist_genres(artist, genres):
    data = [(artist, genres)]
    cur.executemany("INSERT INTO artists_genres (artist, genres) VALUES (?,?)", data)
    con.commit()


def get_genres_local(artist):
    params = (artist,)
    res = cur.execute("SELECT genres from artists_genres where artist = ?", params)
    return res.fetchone()


def get_all_artists(is_xml=False):
    all_artists_list = []
    res = cur.execute("SELECT artists from music_list")
    if is_xml:
        split_char = "&"
    else:
        split_char = ","
    for artists in res.fetchall():
        all_artists_list.extend(artists[0].split(split_char))
    return list(map(str.strip, all_artists_list))


def artists_statistics(all_artists_list):
    return Counter(all_artists_list)


def get_all_genres(all_artists_list):
    all_artists_set = set(all_artists_list)
    for artist in all_artists_set:
        if get_genres_local(artist) is None:
            genres = get_genres_api(artist)
            insert_artist_genres(artist, genres)


def genres_statistics():
    all_genres = []
    res = cur.execute("SELECT genres from music_list")
    for genres in res.fetchall():
        all_genres.extend(genres[0].split(","))
    all_genres = list(map(str.strip, all_genres))
    return Counter(all_genres)


def read_music_list_xml(filename):
    xml_lib = Library(filename)
    for track_id, song in xml_lib.songs.items():
        if hasattr(song, 'genre'):
            insert_track(song.artist, song.name, song.genre)
        else:
            insert_track(song.artist, song.name)


def read_music_list_txt(filename):
    with open(filename, 'r', encoding='UTF-8') as file:
        while line := file.readline():
            split_line = line.split(" - ")
            insert_track(split_line[0], split_line[1].rstrip())


def merge_genres():
    cur.execute("UPDATE music_list SET genres = ifnull(genres,'') || ', ' || (SELECT genres FROM artists_genres WHERE "
                "music_list.artists LIKE '%' || artists_genres.artist || '%') WHERE genres not like '%,%'")
    con.commit()


def add_genres_to_music_list():
    cur.execute("UPDATE music_list SET genres = (SELECT genres FROM artists_genres WHERE "
                "music_list.artists LIKE '%' || artists_genres.artist || '%')")
    con.commit()


def export_stat(data, filename):
    with open(filename, 'w', encoding='UTF-8') as file:
        for key, value in data.most_common():
            if key == '':
                continue
            file.write(f"{key} - {value}\n")


def drop_music_list():
    cur.execute("drop table if exists music_list;")


if __name__ == "__main__":

    if CLEAR_MUSIC_LIST_BEFORE_START:
        drop_music_list()

    create_db()

    if IMPORT_FILE_TYPE == 'xml':
        xml = True
        read_music_list_xml(IMPORT_FILENAME)
    else:
        xml = False
        read_music_list_txt(IMPORT_FILENAME)

    all_artists = get_all_artists(xml)

    artists_statistics = artists_statistics(all_artists)
    print(artists_statistics)
    get_all_genres(all_artists)

    if xml:
        genres_statistics_itunes = genres_statistics()
        print(genres_statistics_itunes)
        export_stat(genres_statistics_itunes, "genres_stat_itunes.txt")

    if xml and MERGE_WITH_ITUNES_GENRE:
        merge_genres()
    else:
        add_genres_to_music_list()

    genres_statistics_all = genres_statistics()
    export_stat(genres_statistics_all, "genres_stat.txt")
    export_stat(artists_statistics, "artists_stat.txt")
    print(genres_statistics_all)
