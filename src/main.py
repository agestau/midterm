
import json
from json import dumps, loads
import numpy as np
import pandas as pd
from typing import Dict, List
from datetime import datetime

from fastapi import FastAPI
from numpy.typing import NDArray

from src.models import book
from src.utils.model_utils import load_model

from src.db.connection import db_connection

import cloudpickle

# loading model
# book_model = load_model("https://www.dropbox.com/s/wmitlsugemqvcb3/book_model.pkl?dl=0")
book_model = cloudpickle.load(open("D:/CA_AI_2022/midterm/book_model.pkl", "rb"))

app = FastAPI()
cursor = db_connection.cursor()

@app.post("/create_books")
def create_books():
    sql = """CREATE TABLE IF NOT EXISTS books (
            id int NOT NULL AUTO_INCREMENT,
            title varchar(255),
            about varchar(5000),
            genre varchar(255),
            PRIMARY KEY(id)
        );"""
    cursor.execute(sql)
    return {"'books' created"}


@app.post("/create_requests")
def create_requests():
    sql = """CREATE TABLE IF NOT EXISTS requests (
            id int NOT NULL AUTO_INCREMENT,
            path varchar(255) NOT NULL,
            input varchar(20000) NOT NULL,
            prediction varchar(255) NOT NULL,
            time timestamp NOT NULL,
            PRIMARY KEY(id)
        );"""
    cursor.execute(sql)
    return {"'requests' created"}


@app.post("/delete_table")
def delete_table():
    sql = "DROP TABLE IF EXISTS requests"
    cursor.execute(sql)
    return {"deleted"}


def insert_df_into_table(dataf):
    cursor = db_connection.cursor()
    lst = dataf.values.tolist()
    sql = 'INSERT INTO books (title, about, genre) VALUES (%s, %s, %s)'
    cursor.executemany(sql, lst)
    db_connection.commit()
    print("DF inserted")
    cursor.close()


# Leaving this in in case I'll ever want to insert a book in the 'books' table
# @app.post("/insert_book")
# def insert_or_update(title, about, genre):
#     cursor = db_connection.cursor()
#     sql = "INSERT INTO books (title, about, genre, prediction) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE prediction = VALUES(prediction)"
#     cursor.execute(sql, (title, about, genre, prediction))
#     db_connection.commit()
#     return {cursor.rowcount, "inserted / updated"}


@app.get("/health")
def health():
    return {"healthy!"}


@app.get("/book")
def get_books():
    cursor = db_connection.cursor()
    sql = "SELECT * from books"
    cursor.execute(sql)
    return [val for val in cursor]


@app.post("/predict_book")
def predict_books(inputs: List[book.Input]) -> Dict[str, List[Dict[str, float]]]:
    parsed_input = pd.DataFrame([i.dict() for i in inputs])
    outputs: NDArray[np.float32] = book_model.predict(parsed_input["text"].values)
    return {"outputs": [{"class": i} for i in outputs.tolist()]}

