import sys
import logging
import os
from sqlite3 import Error
from time import perf_counter
from datetime import timedelta
import mysql.connector

schedule = IntervalSchedule(interval=timedelta(minutes=5))
file_logger = logging.getLogger(__name__)
file_logger.setLevel(logging.INFO)

@task(name="Create a mysql table in google cloud.")
def create_table(book_name):
    """
    Connects to database and then creates a table if one doesn't exist for a specific book.
    Parameters
    ----------
    book_name : String
        String of a book name with a text file of that book.
    Returns
    -------
    None
    """
    
    sql_create_table = f""" CREATE TABLE IF NOT EXISTS {book_name}_table (
                                paragraph_num int NOT NULL AUTO_INCREMENT,
                                paragraph VARCHAR(10000),
                                paragraph_length int,
                                fear int,
                                anger int,
                                anticipation int,
                                trust int,
                                surprise int,
                                positive int,
                                negative int,
                                sadness int,
                                disgust int,
                                joy int,
                                log_runtime float,
                                PRIMARY KEY (`paragraph_num`)
                                ); """

    cnxn = mysql.connector.connect(user = 'root',
        password = 'root',
        host = '34.67.29.181',
        database = 'book-emotions')
        
    if cnxn is not None:
        try:
            print('Connected to MySQL database')
            c = cnxn.cursor()
            c.execute(sql_create_table)
            cnxn.commit()
            return True
        except Error as e:
            print(e)
    else:
        print("Error! Cannot Create the database connection!")

@task(name="Parse book text into array of paragraphs")
def create_data(book_num):
    """
    Function that parses a text file into paragraphs.
    Parameters
    ----------
    book_name : String
        String of a book name with a text file of that book.
    Return
    ------
    A list of paragraphs.
    """
    book_text = gutenbergpy.textget.strip_headers(gutenbergpy.textget.get_text_by_id(book_num))
    paragraphs = []
    tests = book_text.decode("utf-8").split("\n\n")
    for paragraph in tests:
        paragraphs.append(paragraph.replace("\n", " "))

    print("Number of paragraphs in this book: ", len(paragraphs))
    return paragraphs

@task(name="Get emotion score and write it to database")
def insert_data(table_exists, paragraphs, book_name):
    """
    Insert new values into the book's table.
    Parameters
    ----------
    cnxn : cnxn
    project : int, string, int, int, int, int, int, int, int, int, int, int, int, float
        emotion_table values as paragraph number, paragraph, paragraph length, fear, anger, anticipation, trust, surprise, positive, negative, sadness, disgust, joy, log runtime
    Returns
    -------
    None
    """

    if table_exists:
        paragraph_num = 0
        file_handler = logging.FileHandler(f'logs/{book_name}.log')
        file_logger.addHandler(file_handler)

        sql = f'''
            INSERT INTO {book_name}_table (paragraph_num, paragraph, paragraph_length, fear, anger, anticipation, trust, surprise, positive, negative, sadness, disgust, joy, log_runtime)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''
        
        emotion_keys = ["fear", "anger", "anticipation", "trust", "surprise", "positive", "negative", "sadness", "disgust", "joy"]

        cnxn = mysql.connector.connect(user = 'root',
                password = 'root',
                host = '34.67.29.181',
                database = 'book-emotions')
        cursor = cnxn.cursor()

        for paragraph in paragraphs:
            start_time = perf_counter()
            text_object = NRCLex(paragraph)
            emotion_scores = text_object.raw_emotion_scores
            paragraph_num += 1
            
            for key in emotion_keys:
                if key not in emotion_scores.keys():
                    emotion_scores[key] = 0

            emotion_results = (paragraph_num,
                            paragraph,
                            len(paragraph),
                            emotion_scores["fear"],
                            emotion_scores["anger"],
                            emotion_scores["anticipation"],
                            emotion_scores["trust"],
                            emotion_scores["surprise"],
                            emotion_scores["positive"],
                            emotion_scores["negative"],
                            emotion_scores["sadness"],
                            emotion_scores["disgust"],
                            emotion_scores["joy"],
                            0)

            cursor.execute(sql, emotion_results)
            cnxn.commit()

            end_time = perf_counter()
            total_time = end_time - start_time
            try:
                file_logger.info('Paragraph Id: %i, Paragraph Length: %i, Execution Time: %f', paragraph_num, len(paragraph), total_time)
            except Error as e:
                file_logger.error(paragraph_num, e)

            update_sql = f'''
                        UPDATE {book_name}_table
                        SET log_runtime = {total_time}
                        WHERE paragraph_num = {paragraph_num}
                        '''
            
            cursor.execute(update_sql)
            cnxn.commit()
        cnxn.close()

@flow(name = "Emotion Analysis Pipeline")
def main_flow(book_name, book_num):
    try:
        print("Proccessing ", book_name)
        table_exists = create_table(book_name)
        paragraph_data = create_data(book_num)
        insert_data(table_exists, paragraph_data, book_name)
    except Error as e:
        file_logger.error(e)

if __name__ == "__main__":
    main_flow(sys.argv[1], sys.argv[2])